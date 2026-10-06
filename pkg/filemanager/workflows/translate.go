package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs/dbfs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
)

// TranslateTask envia um arquivo ao Transynex (TRANSYNEX_URL, autenticado por
// TRANSYNEX_API_KEY), acompanha a tradução e grava o resultado ao lado do
// original, no mesmo formato.
type (
	TranslateTask struct {
		*queue.DBTask

		l     logging.Logger
		state *TranslateTaskState
	}
	TranslateTaskPhase string
	TranslateTaskState struct {
		Uri            string             `json:"uri"`
		Dst            string             `json:"dst"`
		SourceLanguage string             `json:"source_language"`
		TargetLanguage string             `json:"target_language"`
		ProjectID      string             `json:"project_id,omitempty"`
		Format         string             `json:"format,omitempty"` // "" = página renderizada (imagem avulsa)
		ExportJobID    string             `json:"export_job_id,omitempty"`
		JobsDone       int64              `json:"jobs_done,omitempty"`
		JobsTotal      int64              `json:"jobs_total,omitempty"`
		TempPath       string             `json:"temp_path,omitempty"`
		Phase          TranslateTaskPhase `json:"phase,omitempty"`
	}
)

const (
	TranslateTaskPhaseSubmit    TranslateTaskPhase = ""
	TranslateTaskPhaseTranslate TranslateTaskPhase = "translate"
	TranslateTaskPhaseExport    TranslateTaskPhase = "export"
	TranslateTaskPhaseTransfer  TranslateTaskPhase = "transfer"

	ProgressTypeTranslated = "translated"
	SummaryKeyTargetLang   = "target_language"

	translatePollInterval = 10 * time.Second
)

// Extensão do arquivo gerado por formato de exportação do Transynex.
var translateOutputExt = map[string]string{"": ".png", "pdf": ".pdf", "cbz": ".cbz", "epub": ".epub", "markdown": ".md"}

func init() {
	queue.RegisterResumableTaskFactory(queue.TranslateTaskType, NewTranslateTaskFromModel)
}

func NewTranslateTask(ctx context.Context, src, sourceLang, targetLang string) (queue.Task, error) {
	uri, err := fs.NewUriFromString(src)
	if err != nil {
		return nil, err
	}

	stateBytes, err := json.Marshal(&TranslateTaskState{
		Uri:            src,
		Dst:            uri.DirUri().String(),
		SourceLanguage: sourceLang,
		TargetLanguage: targetLang,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	return &TranslateTask{
		DBTask: &queue.DBTask{
			Task: &ent.Task{
				Type:          queue.TranslateTaskType,
				CorrelationID: logging.CorrelationID(ctx),
				PrivateState:  string(stateBytes),
				PublicState:   &types.TaskPublicState{},
			},
			DirectOwner: inventory.UserFromContext(ctx),
		},
	}, nil
}

func NewTranslateTaskFromModel(task *ent.Task) queue.Task {
	return &TranslateTask{DBTask: &queue.DBTask{Task: task}}
}

func (m *TranslateTask) Do(ctx context.Context) (task.Status, error) {
	dep := dependency.FromContext(ctx)
	m.l = dep.Logger()

	state := &TranslateTaskState{}
	if err := json.Unmarshal([]byte(m.State()), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %w", err)
	}
	m.Lock()
	m.state = state
	m.Unlock()

	api, err := newTransynexClient()
	if err != nil {
		return task.StatusError, err
	}

	var next task.Status
	switch m.state.Phase {
	case TranslateTaskPhaseSubmit:
		next, err = m.submit(ctx, dep, api)
	case TranslateTaskPhaseTranslate:
		next, err = m.monitor(ctx, api)
	case TranslateTaskPhaseExport:
		next, err = m.export(ctx, api)
	case TranslateTaskPhaseTransfer:
		next, err = m.transfer(ctx, dep, api)
	default:
		next, err = task.StatusError, fmt.Errorf("unknown phase %q (%w)", m.state.Phase, queue.CriticalErr)
	}

	newStateStr, marshalErr := json.Marshal(m.state)
	if marshalErr != nil {
		return task.StatusError, fmt.Errorf("failed to marshal state: %w", marshalErr)
	}

	m.Lock()
	m.Task.PrivateState = string(newStateStr)
	m.Unlock()
	return next, err
}

// submit envia o arquivo ao envio rápido do Transynex, que cria o projeto e
// já enfileira a tradução.
func (m *TranslateTask) submit(ctx context.Context, dep dependency.Dep, api *transynexClient) (task.Status, error) {
	uri, err := fs.NewUriFromString(m.state.Uri)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to parse src uri: %s (%w)", err, queue.CriticalErr)
	}

	user := inventory.UserFromContext(ctx)
	fm := manager.NewFileManager(dep, user)
	defer fm.Recycle()

	file, err := fm.Get(ctx, uri, dbfs.WithFileEntities(), dbfs.WithRequiredCapabilities(dbfs.NavigatorCapabilityDownloadFile), dbfs.WithNotRoot())
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to get file: %s (%w)", err, queue.CriticalErr)
	}

	es, err := fm.GetEntitySource(ctx, 0, fs.WithEntity(file.PrimaryEntity()))
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to get entity source: %w", err)
	}
	defer es.Close()

	// Multipart em stream: arquivos grandes não passam inteiros pela memória.
	body, w := io.Pipe()
	form := multipart.NewWriter(w)
	go func() {
		part, err := form.CreateFormFile("file", file.DisplayName())
		if err == nil {
			_, err = io.Copy(part, es)
		}
		if err == nil {
			err = form.Close()
		}
		w.CloseWithError(err)
	}()

	query := url.Values{"sourceLanguage": {m.state.SourceLanguage}, "targetLanguage": {m.state.TargetLanguage}}
	var res struct {
		ProjectID string `json:"projectId"`
		Kind      string `json:"kind"`
	}
	if err := api.do(ctx, http.MethodPost, "/api/v1/quick?"+query.Encode(), form.FormDataContentType(), body, &res); err != nil {
		return task.StatusError, fmt.Errorf("failed to submit file to Transynex: %w", err)
	}

	m.state.ProjectID = res.ProjectID
	m.state.Format = translateFormatFor(res.Kind, file.DisplayName())
	m.state.Phase = TranslateTaskPhaseTranslate
	m.l.Info("File %q submitted to Transynex project %s", uri, res.ProjectID)
	m.ResumeAfter(translatePollInterval)
	return task.StatusSuspending, nil
}

// translateFormatFor devolve o resultado no mesmo formato do original.
func translateFormatFor(kind, name string) string {
	ext := strings.ToLower(path.Ext(name))
	switch {
	case kind == "DOCUMENT" && ext == ".epub":
		return "epub"
	case kind == "DOCUMENT":
		return "markdown"
	case ext == ".pdf":
		return "pdf"
	case ext == ".cbz" || ext == ".zip":
		return "cbz"
	default:
		return ""
	}
}

// monitor espera o projeto sair de DRAFT/PROCESSING: o Transynex só marca
// READY/ERROR quando não resta job pendente.
func (m *TranslateTask) monitor(ctx context.Context, api *transynexClient) (task.Status, error) {
	var project struct {
		Status string `json:"status"`
	}
	if err := api.do(ctx, http.MethodGet, "/api/v1/projects/"+m.state.ProjectID, "", nil, &project); err != nil {
		return task.StatusError, fmt.Errorf("failed to get Transynex project: %w", err)
	}

	var jobs []struct {
		Type   string `json:"type"`
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := api.do(ctx, http.MethodGet, "/api/v1/jobs?projectId="+url.QueryEscape(m.state.ProjectID), "", nil, &jobs); err != nil {
		return task.StatusError, fmt.Errorf("failed to list Transynex jobs: %w", err)
	}
	var done, total int64
	firstErr := ""
	for _, j := range jobs {
		if j.Type == "export" {
			continue
		}
		total++
		if j.Status == "completed" || j.Status == "failed" {
			done++
		}
		if j.Status == "failed" && firstErr == "" {
			firstErr = j.Error
		}
	}
	m.Lock()
	m.state.JobsDone, m.state.JobsTotal = done, total
	m.Unlock()

	switch project.Status {
	case "READY":
		m.state.Phase = TranslateTaskPhaseExport
		if m.state.Format == "" {
			m.state.Phase = TranslateTaskPhaseTransfer
		}
		return task.StatusSuspending, nil
	case "ERROR":
		return task.StatusError, fmt.Errorf("translation failed in Transynex: %s (%w)", firstErr, queue.CriticalErr)
	}

	m.ResumeAfter(translatePollInterval)
	return task.StatusSuspending, nil
}

func (m *TranslateTask) export(ctx context.Context, api *transynexClient) (task.Status, error) {
	if m.state.ExportJobID == "" {
		var res struct {
			JobID string `json:"jobId"`
		}
		payload := strings.NewReader(fmt.Sprintf(`{"format":%q}`, m.state.Format))
		if err := api.do(ctx, http.MethodPost, "/api/v1/projects/"+m.state.ProjectID+"/export", "application/json", payload, &res); err != nil {
			return task.StatusError, fmt.Errorf("failed to request export: %w", err)
		}
		m.state.ExportJobID = res.JobID
		m.ResumeAfter(translatePollInterval)
		return task.StatusSuspending, nil
	}

	var job struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := api.do(ctx, http.MethodGet, "/api/v1/jobs/"+m.state.ExportJobID, "", nil, &job); err != nil {
		return task.StatusError, fmt.Errorf("failed to get export job: %w", err)
	}
	switch job.Status {
	case "completed":
		m.state.Phase = TranslateTaskPhaseTransfer
		return task.StatusSuspending, nil
	case "failed":
		return task.StatusError, fmt.Errorf("export failed in Transynex: %s (%w)", job.Error, queue.CriticalErr)
	}

	m.ResumeAfter(translatePollInterval)
	return task.StatusSuspending, nil
}

// transfer baixa o resultado para a pasta temporária (o upload precisa do
// tamanho) e grava como "<nome>.<idioma><ext>" na pasta do original.
func (m *TranslateTask) transfer(ctx context.Context, dep dependency.Dep, api *transynexClient) (task.Status, error) {
	downloadPath, err := m.resultDownloadPath(ctx, api)
	if err != nil {
		return task.StatusError, err
	}

	if m.state.TempPath == "" {
		if m.state.TempPath, err = prepareTempFolder(ctx, dep, m); err != nil {
			return task.StatusError, err
		}
	}
	tempFile := filepath.Join(m.state.TempPath, "result")
	if err := api.download(ctx, downloadPath, tempFile); err != nil {
		return task.StatusError, fmt.Errorf("failed to download result: %w", err)
	}

	f, err := os.Open(tempFile)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to open result: %w", err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to stat result: %w", err)
	}

	src, _ := fs.NewUriFromString(m.state.Uri)
	dst, err := fs.NewUriFromString(m.state.Dst)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to parse dst uri: %s (%w)", err, queue.CriticalErr)
	}
	base := strings.TrimSuffix(src.Name(), path.Ext(src.Name()))
	name := sanitizeFileName(fmt.Sprintf("%s.%s%s", base, m.state.TargetLanguage, translateOutputExt[m.state.Format]))

	fm := manager.NewFileManager(dep, inventory.UserFromContext(ctx))
	defer fm.Recycle()
	if _, err := fm.Update(ctx, &fs.UploadRequest{
		Props: &fs.UploadProps{Uri: dst.JoinRaw(name), Size: stat.Size()},
		File:  f,
	}, fs.WithNoEntityType()); err != nil {
		return task.StatusError, fmt.Errorf("failed to upload result: %w", err)
	}

	m.l.Info("Translated file saved to %q", dst.JoinRaw(name))
	return task.StatusCompleted, nil
}

func (m *TranslateTask) resultDownloadPath(ctx context.Context, api *transynexClient) (string, error) {
	if m.state.Format == "" {
		var pages []struct {
			RenderedImageUrl string `json:"renderedImageUrl"`
		}
		if err := api.do(ctx, http.MethodGet, "/api/v1/projects/"+m.state.ProjectID+"/pages", "", nil, &pages); err != nil {
			return "", fmt.Errorf("failed to list pages: %w", err)
		}
		if len(pages) == 0 || pages[0].RenderedImageUrl == "" {
			return "", fmt.Errorf("no rendered page found (%w)", queue.CriticalErr)
		}
		return pages[0].RenderedImageUrl, nil
	}

	// Mais recente primeiro
	var exports []struct {
		Format      string `json:"format"`
		DownloadUrl string `json:"downloadUrl"`
	}
	if err := api.do(ctx, http.MethodGet, "/api/v1/projects/"+m.state.ProjectID+"/exports", "", nil, &exports); err != nil {
		return "", fmt.Errorf("failed to list exports: %w", err)
	}
	for _, e := range exports {
		if e.Format == m.state.Format {
			return e.DownloadUrl, nil
		}
	}
	return "", fmt.Errorf("export %q not found (%w)", m.state.Format, queue.CriticalErr)
}

func (m *TranslateTask) Cleanup(ctx context.Context) error {
	if m.state != nil && m.state.TempPath != "" {
		return os.RemoveAll(m.state.TempPath)
	}
	return nil
}

func (m *TranslateTask) Summarize(hasher hashid.Encoder) *queue.Summary {
	if m.state == nil {
		if err := json.Unmarshal([]byte(m.State()), &m.state); err != nil {
			return nil
		}
	}

	return &queue.Summary{
		Phase: string(m.state.Phase),
		Props: map[string]any{
			SummaryKeySrc:        m.state.Uri,
			SummaryKeyDst:        m.state.Dst,
			SummaryKeyTargetLang: m.state.TargetLanguage,
		},
	}
}

func (m *TranslateTask) Progress(ctx context.Context) queue.Progresses {
	m.Lock()
	defer m.Unlock()
	if m.state == nil || m.state.JobsTotal == 0 {
		return nil
	}
	return queue.Progresses{
		ProgressTypeTranslated: &queue.Progress{Total: m.state.JobsTotal, Current: m.state.JobsDone},
	}
}

// --- Cliente HTTP do Transynex ------------------------------------------------

type transynexClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func newTransynexClient() (*transynexClient, error) {
	baseURL := strings.TrimRight(os.Getenv("TRANSYNEX_URL"), "/")
	apiKey := os.Getenv("TRANSYNEX_API_KEY")
	if baseURL == "" || apiKey == "" {
		return nil, fmt.Errorf("TRANSYNEX_URL and TRANSYNEX_API_KEY must be set (%w)", queue.CriticalErr)
	}
	return &transynexClient{baseURL: baseURL, apiKey: apiKey, http: &http.Client{}}, nil
}

func (c *transynexClient) request(ctx context.Context, method, p, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+p, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 {
		defer res.Body.Close()
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(res.Body).Decode(&e)
		err = fmt.Errorf("HTTP %d: %s", res.StatusCode, e.Error)
		// 4xx não melhora tentando de novo (exceto 429)
		if res.StatusCode < 500 && res.StatusCode != http.StatusTooManyRequests {
			err = fmt.Errorf("%w (%w)", err, queue.CriticalErr)
		}
		return nil, err
	}
	return res, nil
}

func (c *transynexClient) do(ctx context.Context, method, p, contentType string, body io.Reader, out any) error {
	res, err := c.request(ctx, method, p, contentType, body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return json.NewDecoder(res.Body).Decode(out)
}

func (c *transynexClient) download(ctx context.Context, p, dst string) error {
	res, err := c.request(ctx, http.MethodGet, p, "", nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, res.Body)
	return err
}

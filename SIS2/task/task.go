package task

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// Status - состояние задачи.
type Status string

const (
	StatusPending    Status = "pending"    // создана, но еще не отправлена в очередь
	StatusQueued     Status = "queued"     // принята пулом и ждет в очереди
	StatusProcessing Status = "processing" // воркер прямо сейчас ее обрабатывает
	StatusCompleted  Status = "completed"  // обработана успешно, результат сохранен
	StatusFailed     Status = "failed"     // обработчик вернул ошибку (только в пуле)
)

// ошибки приложения. возвращаются как обычные error.
var (
	ErrEmptyInput        = errors.New("input text is empty")
	ErrTaskNotFound      = errors.New("task not found")
	ErrInvalidTransition = errors.New("invalid state transition")

	// ошибки пула воркеров (CP2)
	ErrQueueFull          = errors.New("task queue is full")
	ErrPoolNotStarted     = errors.New("worker pool is not started")
	ErrPoolStopped        = errors.New("worker pool is stopped")
	ErrPoolAlreadyRunning = errors.New("worker pool is already started")
	ErrInvalidPoolConfig  = errors.New("invalid worker pool config")
)

// Result - результат анализа текста.
type Result struct {
	Words      int    // количество слов
	Characters int    // количество символов (рун)
	Lines      int    // количество непустых строк
	Text       string // исходное предложение
}

// Task - одна задача: анализ текста.
type Task struct {
	ID     int
	Input  string
	Status Status
	Result Result
	Err    string // текст ошибки, если Status == failed
	Worker int    // номер воркера, который обработал задачу (0 - синхронно)
}

// проверяет входной текст. пустой текст или текст из пробелов - ошибка.
func ValidateInput(text string) error {
	if strings.TrimSpace(text) == "" {
		return ErrEmptyInput
	}
	return nil
}

// Enqueue переводит задачу из pending в queued (задача отправлена в очередь).
func (t *Task) Enqueue() error {
	if t.Status != StatusPending {
		return ErrInvalidTransition
	}
	t.Status = StatusQueued
	return nil
}

// Start переводит задачу в processing.
// Из pending - синхронная обработка (как в CP1), из queued - воркер взял задачу.
func (t *Task) Start() error {
	if t.Status != StatusPending && t.Status != StatusQueued {
		return ErrInvalidTransition
	}
	t.Status = StatusProcessing
	return nil
}

// Complete переводит задачу из processing в completed и сохраняет результат.
func (t *Task) Complete(result Result) error {
	if t.Status != StatusProcessing {
		return ErrInvalidTransition
	}
	t.Result = result
	t.Status = StatusCompleted
	return nil
}

// Fail переводит задачу из processing в failed и сохраняет текст ошибки.
func (t *Task) Fail(err error) error {
	if t.Status != StatusProcessing {
		return ErrInvalidTransition
	}
	t.Err = err.Error()
	t.Status = StatusFailed
	return nil
}

// Processor - интерфейс обработчика задач.
type Processor interface {
	Process(input string) (Result, error)
}

// TextAnalyzer - обработчик, который анализирует текст.
type TextAnalyzer struct{}

// Process считает слова, символы и непустые строки.
func (a TextAnalyzer) Process(input string) (Result, error) {
	if err := ValidateInput(input); err != nil {
		return Result{}, err
	}

	words := strings.Fields(input)

	lines := 0
	for _, line := range strings.Split(input, "\n") {
		if strings.TrimSpace(line) != "" {
			lines++
		}
	}

	result := Result{
		Words:      len(words),
		Characters: utf8.RuneCountInString(input),
		Lines:      lines,
		Text:       input,
	}
	return result, nil
}

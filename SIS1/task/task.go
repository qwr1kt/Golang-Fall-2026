package task

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// Status - состояние задачи.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
)

// ошибки приложения. возвращаются как обычные error.
var (
	ErrEmptyInput        = errors.New("input text is empty")
	ErrTaskNotFound      = errors.New("task not found")
	ErrInvalidTransition = errors.New("invalid state transition")
)

// Result - результат анализа текста.
type Result struct {
	Words      int // количество слов
	Characters int // количество символов (рун)
	Lines      int // количество непустых строк
	Text string // исходное предложение
}

// Task - одна задача: анализ текста.
type Task struct {
	ID     int
	Input  string
	Status Status
	Result Result
}

//проверяет входной текст. пустой текст или текст из пробелов - ошибка.
func ValidateInput(text string) error {
	if strings.TrimSpace(text) == "" {
		return ErrEmptyInput
	}
	return nil
}

// Start переводит задачу из pending в processing.
func (t *Task) Start() error {
	if t.Status != StatusPending {
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

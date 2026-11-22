package task

import (
	"errors"
	"testing"
)

// 1. successful task creation;
func TestCreateSuccess(t *testing.T) {
	s := NewService(TextAnalyzer{})

	created, err := s.Create("Hello Go world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if created.ID != 1 {
		t.Errorf("expected ID 1, got %d", created.ID)
	}
	if created.Input != "Hello Go world" {
		t.Errorf("unexpected input: %q", created.Input)
	}
	if created.Status != StatusPending {
		t.Errorf("expected status pending, got %s", created.Status)
	}
}

// 2. invalid task input;
func TestCreateInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"spaces", "     "},
		{"tabs and newlines", " \t\n "},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewService(TextAnalyzer{})
			_, err := s.Create(tc.input)
			if !errors.Is(err, ErrEmptyInput) {
				t.Errorf("expected ErrEmptyInput, got %v", err)
			}
			if len(s.tasks) != 0 {
				t.Errorf("invalid task must not be stored")
			}
		})
	}
}

// 3. successful task retrieval;
func TestGetSuccess(t *testing.T) {
	s := NewService(TextAnalyzer{})
	created, _ := s.Create("some text")

	got, err := s.Get(created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != created.ID || got.Input != "some text" {
		t.Errorf("got wrong task: %+v", got)
	}
}

// 4. retrieval of a nonexistent task;
func TestGetNotFound(t *testing.T) {
	s := NewService(TextAnalyzer{})

	_, err := s.Get(42)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}

	err = s.Process(42)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound on Process, got %v", err)
	}
}

// 5.successful task processing; 6.correctness of the produced result; 7.correct task state after processing;
func TestProcessResult(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		result Result
	}{
		{"simple", "Hello Go world", Result{Words: 3, Characters: 14, Lines: 1, Text: "Hello Go world"}},
		{"one word", "Go", Result{Words: 1, Characters: 2, Lines: 1, Text: "Go"}},
		{"two lines", "one two\nthree", Result{Words: 3, Characters: 13, Lines: 2, Text: "one two\nthree"}},
		{"extra spaces", "  a   b  ", Result{Words: 2, Characters: 9, Lines: 1, Text: "  a   b  "}},
		{"unicode", "Привет мир", Result{Words: 2, Characters: 10, Lines: 1, Text: "Привет мир"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewService(TextAnalyzer{})
			created, _ := s.Create(tc.input)

			if err := s.Process(created.ID); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got, _ := s.Get(created.ID)
			if got.Result != tc.result {
				t.Errorf("expected result %+v, got %+v", tc.result, got.Result)
			}
			if got.Status != StatusCompleted {
				t.Errorf("expected status completed, got %s", got.Status)
			}
		})
	}
}

// 8. at least one invalid operation or state transition;
func TestProcessTwice(t *testing.T) {
	s := NewService(TextAnalyzer{})
	created, _ := s.Create("hello world")

	if err := s.Process(created.ID); err != nil {
		t.Fatalf("first process failed: %v", err)
	}

	err := s.Process(created.ID)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}

	got, _ := s.Get(created.ID)
	if got.Status != StatusCompleted {
		t.Errorf("status must stay completed, got %s", got.Status)
	}
}

func TestInvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from Status
		op   string
	}{
		{"complete from pending", StatusPending, "complete"},
		{"complete from completed", StatusCompleted, "complete"},
		{"start from processing", StatusProcessing, "start"},
		{"start from completed", StatusCompleted, "start"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := Task{ID: 1, Input: "x", Status: tc.from}
			var err error
			if tc.op == "start" {
				err = task.Start()
			} else {
				err = task.Complete(Result{})
			}
			if !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("expected ErrInvalidTransition, got %v", err)
			}
			if task.Status != tc.from {
				t.Errorf("status changed to %s", task.Status)
			}
		})
	}
}

// 9. multiple independent tasks.
func TestMultipleIndependentTasks(t *testing.T) {
	s := NewService(TextAnalyzer{})

	first, _ := s.Create("one two three")
	second, _ := s.Create("hello")
	third, _ := s.Create("a b")

	if first.ID == second.ID || second.ID == third.ID || first.ID == third.ID {
		t.Fatalf("IDs must be unique: %d %d %d", first.ID, second.ID, third.ID)
	}

	// Обрабатываем только первую и вторую
	s.Process(first.ID)
	s.Process(second.ID)

	g1, _ := s.Get(first.ID)
	g2, _ := s.Get(second.ID)
	g3, _ := s.Get(third.ID)

	if g1.Result.Words != 3 {
		t.Errorf("first: expected 3 words, got %d", g1.Result.Words)
	}
	if g2.Result.Words != 1 {
		t.Errorf("second: expected 1 word, got %d", g2.Result.Words)
	}
	if g3.Status != StatusPending {
		t.Errorf("third must stay pending, got %s", g3.Status)
	}
	if g3.Result != (Result{}) {
		t.Errorf("third must have empty result, got %+v", g3.Result)
	}
	if g1.Input != "one two three" || g2.Input != "hello" {
		t.Errorf("inputs were mixed up")
	}
}

// Мок-процессор: проверяем, что сервис работает с интерфейсом
type failingProcessor struct{}

func (failingProcessor) Process(input string) (Result, error) {
	return Result{}, errors.New("boom")
}

func TestProcessorError(t *testing.T) {
	s := NewService(failingProcessor{})
	created, _ := s.Create("text")

	if err := s.Process(created.ID); err == nil {
		t.Fatal("expected error from processor")
	}

	got, _ := s.Get(created.ID)
	if got.Status != StatusPending {
		t.Errorf("expected status pending after failure, got %s", got.Status)
	}
}

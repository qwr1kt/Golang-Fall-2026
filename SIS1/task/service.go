package task

// Service хранит задачи в памяти и управляет ими
type Service struct {
	tasks     map[int]*Task
	nextID    int
	processor Processor
}

// NewService создает сервис с заданным обработчиком
func NewService(p Processor) *Service {
	return &Service{
		tasks:     make(map[int]*Task),
		nextID:    1,
		processor: p,
	}
}

// Create проверяет входной текст, создает задачу и сохраняет ее
func (s *Service) Create(text string) (Task, error) {
	if err := ValidateInput(text); err != nil {
		return Task{}, err
	}

	t := &Task{
		ID:     s.nextID,
		Input:  text,
		Status: StatusPending,
	}
	s.tasks[t.ID] = t
	s.nextID++

	return *t, nil
}

// Get возвращает копию задачи по ID
func (s *Service) Get(id int) (Task, error) {
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, ErrTaskNotFound
	}
	return *t, nil
}

// Process обрабатывает задачу: pending -> processing -> completed
func (s *Service) Process(id int) error {
	t, ok := s.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}

	if err := t.Start(); err != nil {
		return err
	}

	result, err := s.processor.Process(t.Input)
	if err != nil {
		// обработка не удалась - возвращаем задачу в p ending
		t.Status = StatusPending
		return err
	}

	return t.Complete(result)
}

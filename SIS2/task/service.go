package task

import (
	"sort"
	"sync"
)

// Service хранит задачи в памяти и управляет ими.
// Service безопасен для одновременного использования из нескольких горутин:
// все чтения и записи map и полей задач идут под mu.
type Service struct {
	mu        sync.RWMutex  // защищает tasks, nextID и все поля *Task внутри map
	tasks     map[int]*Task // общее состояние: читают main, тесты и воркеры
	nextID    int
	processor Processor // без состояния, его можно вызывать без блокировки
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

	s.mu.Lock()
	defer s.mu.Unlock()

	t := &Task{
		ID:     s.nextID,
		Input:  text,
		Status: StatusPending,
	}
	s.tasks[t.ID] = t
	s.nextID++ // чтение и увеличение nextID под одной блокировкой - ID уникальны

	return *t, nil
}

// Get возвращает КОПИЮ задачи по ID.
// Копия нужна, чтобы вызывающий код не читал поля, которые в это время меняет воркер.
func (s *Service) Get(id int) (Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tasks[id]
	if !ok {
		return Task{}, ErrTaskNotFound
	}
	return *t, nil
}

// List возвращает копии всех задач, отсортированные по ID.
func (s *Service) List() []Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		list = append(list, *t)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return list
}

// Process - синхронная обработка из CP1: pending -> processing -> completed.
// Если обработчик вернул ошибку, задача возвращается в pending (как в CP1).
func (s *Service) Process(id int) error {
	s.mu.Lock()
	t, ok := s.tasks[id]
	if !ok {
		s.mu.Unlock()
		return ErrTaskNotFound
	}
	// задача в очереди принадлежит пулу - синхронно ее обрабатывать нельзя
	if t.Status == StatusQueued {
		s.mu.Unlock()
		return ErrInvalidTransition
	}
	if err := t.Start(); err != nil {
		s.mu.Unlock()
		return err
	}
	input := t.Input
	s.mu.Unlock()

	// сама обработка - без блокировки, чтобы не мешать другим горутинам
	result, err := s.processor.Process(input)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		// обработка не удалась - возвращаем задачу в pending
		t.Status = StatusPending
		return err
	}
	return t.Complete(result)
}

// enqueue атомарно (под одной блокировкой) проверяет состояние задачи,
// кладет ее ID в очередь без ожидания и переводит в queued.
// Если очередь полна, задача остается pending.
func (s *Service) enqueue(id int, queue chan<- int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	// pending -> queued. queued/processing/completed/failed - ошибка (дубликат)
	if err := t.Enqueue(); err != nil {
		return err
	}

	select {
	case queue <- id: // место в буфере есть - не блокируемся
		return nil
	default: // буфер полон - сразу отказ, задача возвращается в pending
		t.Status = StatusPending
		return ErrQueueFull
	}
}

// processQueued вызывается воркером для задачи, которую он взял из очереди.
// Блокировка держится только на время смены состояния, не на время обработки.
func (s *Service) processQueued(id int, worker int) {
	s.mu.Lock()
	t, ok := s.tasks[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	// queued -> processing. Только один воркер может получить этот ID из канала,
	// но проверка все равно защищает от неверного состояния.
	if t.Status != StatusQueued {
		s.mu.Unlock()
		return
	}
	_ = t.Start() // ошибки быть не может: статус только что проверен
	t.Worker = worker
	input := t.Input
	s.mu.Unlock()

	result, err := s.processor.Process(input)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		_ = t.Fail(err) // processing -> failed
		return
	}
	_ = t.Complete(result) // processing -> completed
}

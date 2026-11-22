package task

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gateProcessor - тестовый обработчик, которым тест управляет вручную:
// при старте каждой задачи отправляет сигнал в started и ждет, пока тест закроет release.
// Так можно надежно поймать момент, когда задачи обрабатываются одновременно,
// без time.Sleep.
type gateProcessor struct {
	started chan string   // сигнал "начал обрабатывать вход X"
	release chan struct{} // закрыть, чтобы отпустить все задачи

	active    atomic.Int32 // сколько задач обрабатывается прямо сейчас
	maxActive atomic.Int32 // максимум одновременно
}

func newGateProcessor() *gateProcessor {
	return &gateProcessor{
		started: make(chan string, 100),
		release: make(chan struct{}),
	}
}

func (g *gateProcessor) Process(input string) (Result, error) {
	n := g.active.Add(1)
	for {
		old := g.maxActive.Load()
		if n <= old || g.maxActive.CompareAndSwap(old, n) {
			break
		}
	}
	g.started <- input
	<-g.release
	g.active.Add(-1)
	return TextAnalyzer{}.Process(input)
}

// waitStarted ждет n сигналов о начале обработки (с таймаутом, чтобы тест не завис).
func waitStarted(t *testing.T, g *gateProcessor, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-g.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d tasks started processing", i, n)
		}
	}
}

// stopWithTimeout вызывает Stop и падает, если воркеры не завершились (deadlock/утечка).
func stopWithTimeout(t *testing.T, p *Pool) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_ = p.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return: workers did not terminate")
	}
}

func newStartedPool(t *testing.T, p Processor, workers, queueSize int) (*Service, *Pool) {
	t.Helper()
	s := NewService(p)
	pool, err := NewPool(s, workers, queueSize)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if err := pool.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return s, pool
}

// 1. успешный запуск пула + проверка конфигурации
func TestPoolStart(t *testing.T) {
	s := NewService(TextAnalyzer{})

	if _, err := NewPool(s, 1, 5); !errors.Is(err, ErrInvalidPoolConfig) {
		t.Errorf("1 worker: expected ErrInvalidPoolConfig, got %v", err)
	}
	if _, err := NewPool(s, 2, 0); !errors.Is(err, ErrInvalidPoolConfig) {
		t.Errorf("queue 0: expected ErrInvalidPoolConfig, got %v", err)
	}

	pool, err := NewPool(s, 3, 5)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	// до Start задачи не принимаются
	created, _ := s.Create("hello")
	if err := pool.Submit(created.ID); !errors.Is(err, ErrPoolNotStarted) {
		t.Errorf("expected ErrPoolNotStarted, got %v", err)
	}

	if err := pool.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := pool.Start(); !errors.Is(err, ErrPoolAlreadyRunning) {
		t.Errorf("second Start: expected ErrPoolAlreadyRunning, got %v", err)
	}
	if pool.Workers() != 3 || pool.QueueCap() != 5 {
		t.Errorf("wrong config: workers=%d cap=%d", pool.Workers(), pool.QueueCap())
	}
	stopWithTimeout(t, pool)
}

// 2. Submit возвращается сразу, не дожидаясь обработки
func TestSubmitIsAsynchronous(t *testing.T) {
	g := newGateProcessor()
	s, pool := newStartedPool(t, g, 2, 5)

	created, _ := s.Create("async text")
	// обработчик заблокирован на release, но Submit все равно должен вернуться
	if err := pool.Submit(created.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitStarted(t, g, 1)

	got, _ := s.Get(created.ID)
	if got.Status != StatusProcessing {
		t.Errorf("expected processing while worker is busy, got %s", got.Status)
	}

	close(g.release)
	stopWithTimeout(t, pool)

	got, _ = s.Get(created.ID)
	if got.Status != StatusCompleted {
		t.Errorf("expected completed, got %s", got.Status)
	}
}

// 3, 5, 6. много задач: все обработаны, у каждой свой правильный результат и статус
func TestProcessManyTasks(t *testing.T) {
	s, pool := newStartedPool(t, TextAnalyzer{}, 4, 100)

	want := make(map[int]Result)
	for i := 1; i <= 50; i++ {
		// у каждой задачи разное число слов: i слов
		text := ""
		for w := 0; w < i; w++ {
			text += fmt.Sprintf("w%d ", w)
		}
		created, err := s.Create(text)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		res, _ := TextAnalyzer{}.Process(text)
		want[created.ID] = res
		if err := pool.Submit(created.ID); err != nil {
			t.Fatalf("Submit %d: %v", created.ID, err)
		}
	}

	stopWithTimeout(t, pool) // Stop дожидается всех принятых задач

	for id, res := range want {
		got, _ := s.Get(id)
		if got.Status != StatusCompleted {
			t.Errorf("task %d: expected completed, got %s", id, got.Status)
		}
		if got.Result != res {
			t.Errorf("task %d: wrong result %+v, want %+v", id, got.Result, res)
		}
		if got.Worker < 1 || got.Worker > 4 {
			t.Errorf("task %d: unexpected worker %d", id, got.Worker)
		}
	}
}

// 4. доказательство параллельности: две задачи одновременно в processing.
// Также проверяем, что одновременно работает не больше воркеров, чем задано.
func TestTasksProcessedConcurrently(t *testing.T) {
	g := newGateProcessor()
	s, pool := newStartedPool(t, g, 2, 10)

	var ids []int
	for _, text := range []string{"a", "b c", "d e f", "g", "h i", "j"} {
		created, _ := s.Create(text)
		ids = append(ids, created.ID)
		if err := pool.Submit(created.ID); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	// оба воркера начали обработку, и ни один еще не закончил -> обработка перекрывается
	waitStarted(t, g, 2)
	processing := 0
	for _, id := range ids {
		got, _ := s.Get(id)
		if got.Status == StatusProcessing {
			processing++
		}
	}
	if processing != 2 {
		t.Errorf("expected exactly 2 tasks in processing at once, got %d", processing)
	}

	close(g.release)
	stopWithTimeout(t, pool)

	if g.maxActive.Load() != 2 {
		t.Errorf("expected max 2 concurrent tasks (2 workers), got %d", g.maxActive.Load())
	}
	for _, id := range ids {
		if got, _ := s.Get(id); got.Status != StatusCompleted {
			t.Errorf("task %d: expected completed, got %s", id, got.Status)
		}
	}
}

// 7. дубликаты и неверные отправки
func TestInvalidAndDuplicateSubmit(t *testing.T) {
	g := newGateProcessor()
	s, pool := newStartedPool(t, g, 2, 5)

	// несуществующая задача
	if err := pool.Submit(999); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}

	a, _ := s.Create("task a")
	if err := pool.Submit(a.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitStarted(t, g, 1)

	// задача в processing - повторная отправка запрещена
	if err := pool.Submit(a.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("submit processing task: expected ErrInvalidTransition, got %v", err)
	}
	// синхронный Process тоже не может взять задачу, которую обрабатывает воркер
	if err := s.Process(a.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("Process processing task: expected ErrInvalidTransition, got %v", err)
	}

	close(g.release)
	stopWithTimeout(t, pool)

	// задача completed - повторная отправка тоже запрещена (нужен новый пул, старый остановлен)
	pool2, _ := NewPool(s, 2, 5)
	_ = pool2.Start()
	if err := pool2.Submit(a.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("submit completed task: expected ErrInvalidTransition, got %v", err)
	}
	stopWithTimeout(t, pool2)

	got, _ := s.Get(a.ID)
	if got.Status != StatusCompleted || got.Result.Words != 2 {
		t.Errorf("task must stay completed with its result, got %+v", got)
	}
}

// 7. одну и ту же задачу одновременно отправляют много горутин -> принимается ровно один раз
func TestConcurrentDuplicateSubmit(t *testing.T) {
	g := newGateProcessor()
	s, pool := newStartedPool(t, g, 4, 10)
	created, _ := s.Create("only once")

	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pool.Submit(created.ID); err == nil {
				ok.Add(1)
			} else if !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != 1 {
		t.Fatalf("task must be accepted exactly once, accepted %d times", ok.Load())
	}
	close(g.release)
	stopWithTimeout(t, pool)

	// каждый вызов Process кладет 1 сигнал в started -> должен быть ровно 1
	if n := len(g.started); n != 1 {
		t.Errorf("task must be processed exactly once, processed %d times", n)
	}
}

// 8. полная очередь: отказ ErrQueueFull, задача остается pending (не queued, не processing)
func TestQueueFull(t *testing.T) {
	g := newGateProcessor()
	s, pool := newStartedPool(t, g, 2, 2)

	var ids []int
	for i := 0; i < 5; i++ {
		created, _ := s.Create(fmt.Sprintf("text %d", i))
		ids = append(ids, created.ID)
	}

	// 2 задачи забирают воркеры и зависают на release
	for _, id := range ids[:2] {
		if err := pool.Submit(id); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	waitStarted(t, g, 2)

	// еще 2 заполняют очередь (cap = 2)
	for _, id := range ids[2:4] {
		if err := pool.Submit(id); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	if pool.QueueLen() != 2 {
		t.Fatalf("expected queue length 2, got %d", pool.QueueLen())
	}

	// 5-я - очередь полна
	err := pool.Submit(ids[4])
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	got, _ := s.Get(ids[4])
	if got.Status != StatusPending {
		t.Errorf("rejected task must stay pending, got %s", got.Status)
	}

	// отпускаем воркеров: очередь освобождается, отклоненную задачу можно отправить снова
	close(g.release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		err = pool.Submit(ids[4])
		if err == nil {
			break
		}
		if !errors.Is(err, ErrQueueFull) || time.Now().After(deadline) {
			t.Fatalf("resubmit failed: %v", err)
		}
		runtime.Gosched()
	}

	stopWithTimeout(t, pool)
	for _, id := range ids {
		if got, _ := s.Get(id); got.Status != StatusCompleted {
			t.Errorf("task %d: expected completed, got %s", id, got.Status)
		}
	}
}

// ошибка обработчика в пуле -> failed, а не completed
func TestWorkerProcessorError(t *testing.T) {
	s, pool := newStartedPool(t, failingProcessor{}, 2, 5)
	created, _ := s.Create("text")

	if err := pool.Submit(created.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	stopWithTimeout(t, pool)

	got, _ := s.Get(created.ID)
	if got.Status != StatusFailed {
		t.Errorf("expected failed, got %s", got.Status)
	}
	if got.Err != "boom" || got.Result != (Result{}) {
		t.Errorf("failed task must keep error and empty result, got %+v", got)
	}
}

// 9. безопасный параллельный доступ: воркеры пишут, другие горутины создают и читают.
// Запускать с -race.
func TestConcurrentAccess(t *testing.T) {
	s, pool := newStartedPool(t, TextAnalyzer{}, 4, 200)

	var wg sync.WaitGroup
	results := make(chan int, 200) // ID принятых задач

	// 4 горутины создают и отправляют задачи
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				created, err := s.Create(fmt.Sprintf("goroutine %d task %d", g, i))
				if err != nil {
					t.Errorf("Create: %v", err)
					return
				}
				if err := pool.Submit(created.ID); err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
				results <- created.ID
			}
		}(g)
	}
	// 4 горутины одновременно читают задачи
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				for _, task := range s.List() {
					if _, err := s.Get(task.ID); err != nil {
						t.Errorf("Get: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(results)
	stopWithTimeout(t, pool)

	seen := make(map[int]bool)
	for id := range results {
		if seen[id] {
			t.Errorf("duplicate ID %d", id)
		}
		seen[id] = true
		got, _ := s.Get(id)
		if got.Status != StatusCompleted || got.Result.Words != 4 || got.Result.Text != got.Input {
			t.Errorf("task %d has wrong state/result: %+v", id, got)
		}
	}
	if len(seen) != 100 {
		t.Errorf("expected 100 tasks, got %d", len(seen))
	}
}

// 10. воркеры завершаются после Stop, а принятые задачи доделываются (не теряются)
func TestStopDrainsQueueAndStopsWorkers(t *testing.T) {
	before := runtime.NumGoroutine()

	g := newGateProcessor()
	s, pool := newStartedPool(t, g, 2, 10)

	var ids []int
	for i := 0; i < 8; i++ {
		created, _ := s.Create(fmt.Sprintf("drain %d", i))
		ids = append(ids, created.ID)
		if err := pool.Submit(created.ID); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	waitStarted(t, g, 2) // 2 в работе, 6 ждут в очереди

	// Stop вызывается, пока задачи еще в очереди
	stopped := make(chan struct{})
	go func() {
		_ = pool.Stop()
		close(stopped)
	}()

	// пока воркеры заняты, Stop не должен вернуться
	select {
	case <-stopped:
		t.Fatal("Stop returned before accepted tasks were finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(g.release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return")
	}

	// ни одна принятая задача не потеряна
	for _, id := range ids {
		if got, _ := s.Get(id); got.Status != StatusCompleted {
			t.Errorf("task %d lost: status %s", id, got.Status)
		}
	}

	// повторный Stop безопасен
	stopWithTimeout(t, pool)

	// горутины воркеров завершились (число горутин вернулось к исходному)
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
		}
		runtime.Gosched()
	}
}

// 11. после Stop новые задачи отклоняются, задача остается pending; Start после Stop - ошибка
func TestSubmitAfterStop(t *testing.T) {
	s, pool := newStartedPool(t, TextAnalyzer{}, 2, 5)
	stopWithTimeout(t, pool)

	created, _ := s.Create("too late")
	if err := pool.Submit(created.ID); !errors.Is(err, ErrPoolStopped) {
		t.Errorf("expected ErrPoolStopped, got %v", err)
	}
	if got, _ := s.Get(created.ID); got.Status != StatusPending {
		t.Errorf("rejected task must stay pending, got %s", got.Status)
	}
	if err := pool.Start(); !errors.Is(err, ErrPoolStopped) {
		t.Errorf("Start after Stop: expected ErrPoolStopped, got %v", err)
	}
}

// Submit и Stop одновременно: нет panic "send on closed channel",
// каждая принятая задача обработана, каждая отклоненная осталась pending.
func TestConcurrentSubmitAndStop(t *testing.T) {
	for round := 0; round < 20; round++ {
		s, pool := newStartedPool(t, TextAnalyzer{}, 3, 50)

		var ids []int
		for i := 0; i < 40; i++ {
			created, _ := s.Create(fmt.Sprintf("race %d", i))
			ids = append(ids, created.ID)
		}

		var wg sync.WaitGroup
		var mu sync.Mutex
		accepted := make(map[int]bool)
		for _, id := range ids {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				err := pool.Submit(id)
				if err == nil {
					mu.Lock()
					accepted[id] = true
					mu.Unlock()
				} else if !errors.Is(err, ErrPoolStopped) {
					t.Errorf("unexpected error: %v", err)
				}
			}(id)
		}
		// два Stop одновременно с отправками
		wg.Add(2)
		go func() { defer wg.Done(); _ = pool.Stop() }()
		go func() { defer wg.Done(); _ = pool.Stop() }()
		wg.Wait()

		for _, id := range ids {
			got, _ := s.Get(id)
			if accepted[id] && got.Status != StatusCompleted {
				t.Fatalf("accepted task %d lost: %s", id, got.Status)
			}
			if !accepted[id] && got.Status != StatusPending {
				t.Fatalf("rejected task %d must be pending, got %s", id, got.Status)
			}
		}
	}
}

// Stop без Start не зависает и не паникует
func TestStopWithoutStart(t *testing.T) {
	s := NewService(TextAnalyzer{})
	pool, _ := NewPool(s, 2, 1)
	stopWithTimeout(t, pool)
	stopWithTimeout(t, pool)
}

// новые переходы состояний (CP2)
func TestNewStateTransitions(t *testing.T) {
	task := Task{ID: 1, Input: "x", Status: StatusPending}
	if err := task.Enqueue(); err != nil || task.Status != StatusQueued {
		t.Fatalf("pending -> queued failed: %v", err)
	}
	if err := task.Enqueue(); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("queued -> queued must fail, got %v", err)
	}
	if err := task.Start(); err != nil || task.Status != StatusProcessing {
		t.Fatalf("queued -> processing failed: %v", err)
	}
	if err := task.Fail(errors.New("x")); err != nil || task.Status != StatusFailed {
		t.Fatalf("processing -> failed failed: %v", err)
	}
	for _, op := range []func() error{task.Enqueue, task.Start, func() error { return task.Complete(Result{}) }} {
		if err := op(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("transition from failed must fail, got %v", err)
		}
	}

	pending := Task{Status: StatusPending}
	if err := pending.Fail(errors.New("x")); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("pending -> failed must fail, got %v", err)
	}
}

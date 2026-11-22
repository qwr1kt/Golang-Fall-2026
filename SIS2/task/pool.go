package task

import "sync"

// состояние пула
type poolState int

const (
	poolNew     poolState = iota // создан, воркеры еще не запущены
	poolRunning                  // воркеры работают, Submit принимает задачи
	poolStopped                  // Stop вызван: новые задачи не принимаются
)

// Pool - пул воркеров: фиксированное число горутин читает ID задач
// из одной ограниченной очереди (буферизованного канала).
//
//	Submit(id) --> [ queue: chan int, cap = queueSize ] --> worker 1..N --> Service
//
// Владелец канала queue - Pool: только Submit пишет в него
// и только Stop его закрывает (один раз, под mu).
type Pool struct {
	service *Service
	workers int      // сколько воркеров запустить
	queue   chan int // ограниченная очередь ID задач

	mu    sync.Mutex // защищает state и порядок "отправка в queue / close(queue)"
	state poolState
	wg    sync.WaitGroup // ждем завершения всех воркеров в Stop
}

// NewPool создает пул. workers >= 2 по заданию, queueSize >= 1.
func NewPool(s *Service, workers, queueSize int) (*Pool, error) {
	if s == nil || workers < 2 || queueSize < 1 {
		return nil, ErrInvalidPoolConfig
	}
	return &Pool{
		service: s,
		workers: workers,
		queue:   make(chan int, queueSize),
	}, nil
}

// Start запускает ровно p.workers горутин-воркеров.
func (p *Pool) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch p.state {
	case poolRunning:
		return ErrPoolAlreadyRunning
	case poolStopped:
		return ErrPoolStopped
	}

	for i := 1; i <= p.workers; i++ {
		p.wg.Add(1) // Add ДО запуска горутины, иначе Wait может не дождаться
		go p.worker(i)
	}
	p.state = poolRunning
	return nil
}

// worker берет задачи из очереди, пока канал не закрыт и не пуст.
// range по каналу сам завершится после close(queue), когда буфер опустеет,
// поэтому все принятые задачи будут обработаны.
func (p *Pool) worker(n int) {
	defer p.wg.Done()
	for id := range p.queue {
		p.service.processQueued(id, n)
	}
}

// Submit отправляет задачу в очередь и сразу возвращается (не ждет обработки).
// Если очередь полна - ErrQueueFull (не блокируемся), задача остается pending.
func (p *Pool) Submit(id int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// пока держим mu, Stop не может закрыть канал -> нет panic "send on closed channel"
	switch p.state {
	case poolNew:
		return ErrPoolNotStarted
	case poolStopped:
		return ErrPoolStopped
	}
	return p.service.enqueue(id, p.queue)
}

// Stop: 1) перестает принимать задачи, 2) закрывает очередь,
// 3) ждет, пока воркеры доделают все уже принятые задачи и завершатся.
// Повторный вызов Stop безопасен: канал второй раз не закрывается,
// вызов просто ждет тех же воркеров и возвращает nil.
func (p *Pool) Stop() error {
	p.mu.Lock()
	if p.state == poolStopped {
		p.mu.Unlock()
		p.wg.Wait()
		return nil
	}
	p.state = poolStopped
	close(p.queue) // единственное место, где канал закрывается
	p.mu.Unlock()  // отпускаем mu ДО Wait, чтобы Submit не завис на блокировке

	p.wg.Wait() // ждем выхода всех воркеров (после Stop без Start воркеров 0)
	return nil
}

// QueueLen - сколько задач сейчас ждет в очереди (для демо и тестов).
func (p *Pool) QueueLen() int { return len(p.queue) }

// QueueCap - емкость очереди.
func (p *Pool) QueueCap() int { return cap(p.queue) }

// Workers - число воркеров.
func (p *Pool) Workers() int { return p.workers }

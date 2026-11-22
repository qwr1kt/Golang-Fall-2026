package main

import (
	"errors"
	"fmt"
	"time"

	"sis2/task"
)

type slowAnalyzer struct {
	delay time.Duration
}

func (a slowAnalyzer) Process(input string) (task.Result, error) {
	time.Sleep(a.delay)
	return task.TextAnalyzer{}.Process(input)
}

func main() {
	service := task.NewService(slowAnalyzer{delay: 200 * time.Millisecond})

	texts := []string{
		"Hello World Go",
		"You met me in a very Chinese time in my life",
		"   ",
		"Go channels\nand goroutines",
		"one two three four five",
		"Привет мир",
		"worker pool demo",
		"last task",
	}
	var ids []int
	for _, text := range texts {
		created, err := service.Create(text)
		if err != nil {
			fmt.Printf("create %q failed: %v\n", text, err)
			continue
		}
		ids = append(ids, created.ID)
	}
	fmt.Printf("created %d tasks\n", len(ids))

	// запуск пул: 2 воркера, очередь на 3 задачи
	pool, err := task.NewPool(service, 2, 3)
	if err != nil {
		fmt.Println("new pool:", err)
		return
	}
	if err := pool.Start(); err != nil {
		fmt.Println("start:", err)
		return
	}
	fmt.Printf("pool started: workers=%d, queue capacity=%d\n", pool.Workers(), pool.QueueCap())

	// отправляем все задачи сразу. Submit не ждет обработки.
	// В очередь влезает 3 задачи (+ те, что воркеры успели забрать),
	// остальные сразу получают ErrQueueFull и остаются pending.
	var rejected []int
	for _, id := range ids {
		if err := pool.Submit(id); err != nil {
			fmt.Printf("submit task %d: %v\n", id, err)
			rejected = append(rejected, id)
			continue
		}
		fmt.Printf("submit task %d: ok (queue %d/%d)\n", id, pool.QueueLen(), pool.QueueCap())
	}

	// повторная отправка той же задачи - ошибка (защита от дублей)
	if err := pool.Submit(ids[0]); err != nil {
		fmt.Printf("submit task %d again: %v\n", ids[0], err)
	}

	// читаем задачи, пока воркеры работают 
	time.Sleep(100 * time.Millisecond) // даем воркерам взять задачи, чтобы увидеть processing
	fmt.Println("snapshot while workers are busy:")
	printStatuses(service)

	// ждем, пока освободится место, и повторяем отклоненные задачи
	for _, id := range rejected {
		for {
			err := pool.Submit(id)
			if err == nil {
				fmt.Printf("retry task %d: ok\n", id)
				break
			}
			if !errors.Is(err, task.ErrQueueFull) {
				fmt.Printf("retry task %d: %v\n", id, err)
				break
			}
			time.Sleep(50 * time.Millisecond) // в демо просто ждем места в очереди
		}
	}

	// остановка: новые задачи не принимаются, принятые доделываются
	_ = pool.Stop() // Stop всегда возвращает nil, ждет воркеров
	fmt.Println("pool stopped, all workers finished")

	extra, _ := service.Create("too late")
	if err := pool.Submit(extra.ID); err != nil {
		fmt.Printf("submit task %d after stop: %v\n", extra.ID, err)
	}

	// несуществующая задача
	if _, err := service.Get(99); err != nil {
		fmt.Printf("get 99: %v\n", err)
	}

	// итог
	fmt.Println("final results:")
	for _, t := range service.List() {
		fmt.Printf("task %d [%s] worker=%d: words=%d, characters=%d, lines=%d, text=%q\n",
			t.ID, t.Status, t.Worker, t.Result.Words, t.Result.Characters, t.Result.Lines, t.Input)
	}
}

func printStatuses(s *task.Service) {
	for _, t := range s.List() {
		fmt.Printf("  task %d: %s\n", t.ID, t.Status)
	}
}

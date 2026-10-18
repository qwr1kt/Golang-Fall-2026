package main

import (
	"fmt"

	"sis1/task"
)

func main() {
	service := task.NewService(task.TextAnalyzer{})

	// создаем задачи
	texts := []string{"Hello World Go", "You met me in a very Chinese time in my life", "   "}
	for _, text := range texts {
		created, err := service.Create(text)
		if err != nil {
			fmt.Printf("create %q failed: %v\n", text, err)
			continue
		}
		fmt.Printf("created task %d (status: %s)\n", created.ID, created.Status)
	}

	// обрабатываем задачи 1 и 2
	for id := 1; id <= 2; id++ {
		if err := service.Process(id); err != nil {
			fmt.Printf("process %d failed: %v\n", id, err)
		}
	}

	// повторная обротка - ошибка
	if err := service.Process(1); err != nil {
		fmt.Printf("process 1 again: %v\n", err)
	}

	// несуществующая задача
	if _, err := service.Get(99); err != nil {
		fmt.Printf("get 99: %v\n", err)
	}

	// выводим результаты
	for id := 1; id <= 2; id++ {
		t, err := service.Get(id)
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Printf("task %d [%s]: words=%d, characters=%d, lines=%d, text =%q\n",
			t.ID, t.Status, t.Result.Words, t.Result.Characters, t.Result.Lines, t.Result.Text)
	}
}

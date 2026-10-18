# Golang-Fall-2026
# SIS Project - Checkpoint 1: Text Analyzer

A small synchronous Go application that stores tasks in memory.
**Task** = text analysis. Result: the number of words, characters, and non-empty lines.

Example: `"Hello Go world"' -> words: 3, characters: 14, lines: 1.

## Structure

```
.
├── go.mod
├── main.go # demo launch
├── README.md
└── task/
    ├── task.go # Task, Status, errors, validation, Processor interface, TextAnalyzer
    ├── service.go # Service: storing tasks in map, Create/Get/Process
    └── task_test.go # unit tests
```

## How to launch

```
go run .
go test ./...
```

Pre-delivery checks:

```
gofmt -l .
go vet ./...
go test ./...
```

## Features

- create a task with text (ID is issued automatically: 1, 2, 3...);
- validation: empty text or text with only spaces - error `ErrEmptyInput`;
- getting a task by ID (non-existent - `ErrTaskNotFound`);
- task processing (counting words, characters, strings) and saving the result in the task;
- invalid state transitions - `ErrInvalidTransition`;
- multiple tasks are stored independently in the `map[int]*Task'.

## Task lifecycle

```
pending --Start()--> processing --Complete()--> completed
```

| From | To | Allowed |
|-------------|-------------|-----------|
| pending | processing | yes |
| processing | completed | yes |
| any other transition | none (`ErrInvalidTransition`) |

Re-processing an already completed task is an error.
If the handler returns an error, the task returns to `pending'.

## Interface

`Processor` (`Process(input string) (Result, error)`) is the task handler.
Implementation: 'TextAnalyzer'. The tests use a fake implementation that always returns an error.
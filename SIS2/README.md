# SIS Project - Checkpoint 2: Concurrent Text Analyzer

This is the Checkpoint 1 **Text Analyzer** extended with **concurrent processing**.
The domain and the processing logic are the same as in CP1:

**Task** = text analysis. Result: the number of words, characters (runes), and non-empty lines.

Example: `"Hello Go world"` -> words: 3, characters: 14, lines: 1.

What is new in CP2: 
> SIS2 is a copy of SIS1 that was then extended (module renamed `sis1` -> `sis2`; the `TextAnalyzer`, validation, IDs, storage and all CP1 tests are kept).
> tasks are no longer only processed in the caller's goroutine.
>They can be **submitted** to a bounded queue and processed by a fixed **worker pool**.

The CP1 version (`Service.Process`) is still there and still works.

## Structure

```
.
├── go.mod
├── main.go            # demo: pool with 2 workers, queue of 3, full queue, duplicates, stop
├── README.md
└── task/
    ├── task.go        # Task, Status, errors, validation, state transitions, Processor, TextAnalyzer
    ├── service.go     # Service: task storage (map + RWMutex), Create/Get/List/Process, enqueue/processQueued
    ├── pool.go        # Pool: worker pool, bounded queue, Start/Submit/Stop
    ├── task_test.go   # CP1 unit tests (unchanged)
    └── pool_test.go   # CP2 concurrency tests
```

## How to run

```
go run .
```

## How to run the tests:

```
go test ./...
go test -race ./...
go test -v -race -count=3 ./...   # verbose, several times
```

Checks before submission:

```
gofmt -l .     # prints nothing
go vet ./...
go test ./...
go test -race ./...
```

Requires Go 1.27 (see `go.mod`).

## How it works

### Creating and submitting tasks

```go
service := task.NewService(task.TextAnalyzer{})
pool, err := task.NewPool(service, 2, 3) // 2 workers, queue capacity 3
pool.Start()

t, err := service.Create("Hello Go world") // validated, gets a unique ID, status pending
err = pool.Submit(t.ID)                     // returns immediately, does not wait for processing
got, err := service.Get(t.ID)               // safe to call at any time, returns a copy
pool.Stop()                                 // stop accepting, finish queued tasks, wait for workers
```

`Submit` never waits for processing: it only puts the task ID into the queue and returns.

### Architecture

```
                         Pool (owner of the queue channel)
                 +----------------------------------------------+
 Submit(id) ---> | queue: chan int  (buffered, cap = queueSize)  |
 (non-blocking)  +----------------------------------------------+
                        |              |               |
                        v              v               v
                    worker 1       worker 2   ...  worker N        (fixed N >= 2, started in Start)
                        |              |               |
                        +------+-------+-------+-------+
                               |               |
                               v               v
                 Service: map[int]*Task protected by sync.RWMutex
                 (status, result, error, worker number of every task)
```

* **Number of workers:** set in `NewPool(service, workers, queueSize)`, minimum 2.
  The demo uses **2 workers**. Exactly that many goroutines are started in `Start`; no goroutine is started per task.
* **Queue capacity:** the queue is a buffered channel `make(chan int, queueSize)`, minimum 1.
  The demo uses **capacity 3**. It never grows.
* The queue holds task **IDs**, not task pointers. The task data stays in the `Service` map and is always accessed under the mutex.
* Each worker is a loop `for id := range queue { service.processQueued(id, n) }`. A worker stays
  alive and takes the next task after finishing one. It exits only when the queue is closed **and** empty.

### What happens when the queue is full

Option A from the spec: **the submission is rejected**. `Submit` uses a non-blocking send:

```go
select {
case queue <- id: // there is space
default:          // buffer is full
    return ErrQueueFull
}
```

The caller gets `ErrQueueFull` immediately, the task stays **pending** (never queued/processing),
and it can be submitted again later when workers free some space (the demo does this).

### Task lifecycle

```
            Submit                worker takes it            success
 pending ----------> queued ---------------------> processing -------> completed
    |                                                   |
    |  Service.Process (sync, CP1)                      | processor error (pool)
    +-------------------------------> processing        +---------------> failed
```

| From       | To         | Who / when                                      |
|------------|------------|-------------------------------------------------|
| pending    | queued     | `Pool.Submit`, only if there is space in queue  |
| queued     | processing | a worker took the ID from the queue             |
| pending    | processing | `Service.Process` (synchronous CP1 API)         |
| processing | completed  | processing succeeded, result is stored          |
| processing | failed     | processor returned an error (worker pool)       |
| processing | pending    | processor returned an error in sync `Process` (CP1 behaviour) |

Everything else returns `ErrInvalidTransition`:
submitting a task that is queued, processing, completed or failed (duplicate submission),
processing a completed task again, sync-processing a queued task, etc.
`completed` and `failed` are final; a failed task is not retried automatically.
A failed task is never marked completed and keeps the error text in `Task.Err`.

### Errors

| Error                   | When                                              |
|-------------------------|---------------------------------------------------|
| `ErrEmptyInput`         | text is empty or only whitespace                  |
| `ErrTaskNotFound`       | no task with this ID                              |
| `ErrInvalidTransition`  | wrong state (duplicate submit, re-processing ...) |
| `ErrQueueFull`          | queue is full, submission rejected                |
| `ErrPoolNotStarted`     | `Submit` before `Start`                           |
| `ErrPoolStopped`        | `Submit`/`Start` after `Stop`                     |
| `ErrPoolAlreadyRunning` | `Start` called twice                              |
| `ErrInvalidPoolConfig`  | fewer than 2 workers or queue capacity < 1        |

No `panic` is used for expected errors.

### How shared data is synchronized

| Shared data | Who uses it | Protected by |
|---|---|---|
| `Service.tasks` map, `nextID`, and **all fields of each `*Task`** (status, result, error, worker) | main/test goroutines (`Create`, `Get`, `List`, `Process`), workers (`processQueued`), `Submit` | `Service.mu` (`sync.RWMutex`): `Lock` for writes, `RLock` for `Get`/`List` |
| `Pool.state` (new / running / stopped) and the "send to queue" vs. "close queue" order | `Submit`, `Start`, `Stop` | `Pool.mu` (`sync.Mutex`) |
| the queue itself | `Submit` (sender), workers (receivers) | it is a channel - safe by itself |
| "all workers have exited" | `Stop` | `Pool.wg` (`sync.WaitGroup`) |

Important details:

* The mutex protects not only the map but also the `Task` structs inside it (they are pointers),
  so every read/write of a task field happens under `Service.mu`.
* `Get` and `List` return **copies** of tasks, so callers never read memory a worker is writing.
* **Check-and-change is atomic.** `Submit` checks "status is pending", sends to the queue and sets
  `queued` inside one critical section. Two goroutines submitting the same task cannot both succeed.
  A worker checks "status is queued" and sets `processing` under the lock too.
  So the same task can never be processed by two workers at once (also, a channel value
  is received by exactly one receiver).
* The lock is **not** held during the actual text analysis. A worker locks, switches
  queued -> processing, copies the input, unlocks, analyzes, then locks again to store the result.
  This way slow processing does not block `Get` or other workers.
* `Create` reads and increments `nextID` under the same lock, so IDs are unique even with concurrent `Create`.

### How workers are stopped (shutdown semantics)

`Stop()`:

1. locks `Pool.mu`, sets state to **stopped** - from now on `Submit` returns `ErrPoolStopped`;
2. closes the queue channel (the only place it is closed) and unlocks;
3. workers keep receiving the tasks that are still in the buffer - **accepted tasks are not lost**;
4. when the buffer is empty, `range` ends and every worker calls `wg.Done()`;
5. `Stop` waits in `wg.Wait()` and returns only after all workers exited.

Calling `Stop` again is safe: the channel is not closed twice, the call just waits for
the workers and returns `nil`. `Stop` before `Start` is also safe. A stopped pool cannot be
restarted (`Start` returns `ErrPoolStopped`); create a new pool instead.

Channel ownership: the **Pool** owns the queue. Only `Submit` sends to it and only `Stop` closes it.
Workers only receive.

Forced cancellation of running tasks (`context`) is not part of CP2 - it comes in Checkpoint 3.
A task that is already running is always allowed to finish.

### Why it is race-free and deadlock-free

* **No data races:** every access to the map and task fields goes through `Service.mu`;
  pool state goes through `Pool.mu`; tests pass with `go test -race`.
* **No "send on closed channel" panic:** `Submit` sends while holding `Pool.mu` and checks
  `state == running` first; `Stop` closes the channel while holding the same mutex after
  setting `state = stopped`. So a send and the close can never overlap.
* **Submit never blocks:** the send is non-blocking (`select` with `default`), so holding
  locks during it is safe, and a caller can never hang on a stopped or full pool.
* **Fixed lock order:** the only place that holds two locks is `Submit`: `Pool.mu` then `Service.mu`.
  Workers take only `Service.mu` and never `Pool.mu`. No cycle -> no deadlock.
* **Stop does not hold `Pool.mu` while waiting** in `wg.Wait()`, and workers do not need `Pool.mu`
  to finish, so `Stop` cannot deadlock with the workers.
* **No goroutine leaks:** closing the queue ends every worker's `range`; `wg.Add` is called before
  `go worker()` so `Wait` always counts all of them. A test checks the goroutine count returns to the start value.

## Tests

CP1 tests (`task_test.go`) are unchanged and still pass. CP2 tests (`pool_test.go`):

| Spec requirement | Test |
|---|---|
| 1. worker pool startup | `TestPoolStart` (also invalid config, double Start, Submit before Start) |
| 2. asynchronous submission | `TestSubmitIsAsynchronous` (Submit returns while the worker is blocked) |
| 3. processing many tasks | `TestProcessManyTasks` (50 tasks, 4 workers) |
| 4. real overlapping processing | `TestTasksProcessedConcurrently` (both workers are inside `Process` at the same time; max concurrency = number of workers) |
| 5. correct results | `TestProcessManyTasks`, `TestConcurrentAccess` |
| 6. correct states | all tests check `pending/queued/processing/completed/failed` |
| 7. invalid / duplicate submissions | `TestInvalidAndDuplicateSubmit`, `TestConcurrentDuplicateSubmit` (50 goroutines submit the same task, accepted exactly once, processed once) |
| 8. full queue | `TestQueueFull` (`ErrQueueFull`, task stays pending, can be resubmitted later) |
| 9. safe concurrent access | `TestConcurrentAccess` (create/submit/get/list from many goroutines, run with `-race`) |
| 10. worker termination | `TestStopDrainsQueueAndStopsWorkers` (Stop waits for queued tasks, none lost, no goroutine leak), `TestStopWithoutStart` |
| 11. rejection after shutdown | `TestSubmitAfterStop`, `TestConcurrentSubmitAndStop` (Submit racing with two Stop calls: no panic, accepted tasks completed, rejected ones pending) |
| processing failure | `TestWorkerProcessorError` (status `failed`, not `completed`) |
| state machine | `TestNewStateTransitions` |

The concurrency tests do not depend on the order in which tasks finish. They use a test
processor (`gateProcessor`) that reports "started" on a channel and waits until the test closes
a `release` channel, so overlapping processing is proven with channels, not with `time.Sleep`.

## Demo output (`go run .`)

`main.go` wraps `TextAnalyzer` in a `slowAnalyzer` with a 200 ms delay **only for the demo**, so
the queue visibly fills up. The library code itself never sleeps. The exact worker numbers can differ between runs.

```
create "   " failed: input text is empty
created 7 tasks
pool started: workers=2, queue capacity=3
submit task 1: ok (queue 1/3)
submit task 2: ok (queue 2/3)
submit task 3: ok (queue 3/3)
submit task 4: task queue is full
submit task 5: task queue is full
submit task 6: task queue is full
submit task 7: task queue is full
submit task 1 again: invalid state transition
snapshot while workers are busy:
  task 1: processing
  task 2: processing
  task 3: queued
  task 4: pending
  task 5: pending
  task 6: pending
  task 7: pending
retry task 4: ok
retry task 5: ok
retry task 6: ok
retry task 7: ok
pool stopped, all workers finished
submit task 8 after stop: worker pool is stopped
get 99: task not found
final results:
task 1 [completed] worker=1: words=3, characters=14, lines=1, text="Hello World Go"
task 2 [completed] worker=2: words=11, characters=44, lines=1, text="You met me in a very Chinese time in my life"
task 3 [completed] worker=2: words=4, characters=26, lines=2, text="Go channels\nand goroutines"
task 4 [completed] worker=1: words=5, characters=23, lines=1, text="one two three four five"
task 5 [completed] worker=1: words=2, characters=10, lines=1, text="Привет мир"
task 6 [completed] worker=2: words=3, characters=16, lines=1, text="worker pool demo"
task 7 [completed] worker=1: words=2, characters=9, lines=1, text="last task"
task 8 [pending] worker=0: words=0, characters=0, lines=0, text="too late"
```

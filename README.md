# KotlinStyleCoroutines for .NET 11

A single-file, zero-dependency C# library (`KotlinStyleCoroutines.cs`) bringing **Kotlin structured concurrency**, **custom cancellable continuations**, and **Flow / StateFlow / SharedFlow operators** to .NET 11 (`C# 15`) on top of `Task`, `IAsyncEnumerable<T>`, and `System.Threading.Channels`.

---

## Quick Start

1. Drop `KotlinStyleCoroutines.cs` into any **.NET 11** project.
2. Add `using KotlinStyleCoroutines;` to your source files.
3. To run the xUnit test suite, place `KotlinStyleCoroutines.cs`, `KotlinStyleCoroutinesTests.cs`, and `KotlinStyleCoroutines.Tests.csproj` in a directory and run:
   ```bash
   dotnet test
   ```

---

## API Cheat Sheet (Kotlin -> C#)

### 1. Structured Concurrency & Scopes

| Kotlin | C# (`KotlinStyleCoroutines`) | Behavior |
| :--- | :--- | :--- |
| `coroutineScope { ... }` | `await Coroutine.Scope(async scope => { ... }, ct)` | Waits for all child coroutines; cancels siblings if any child throws. |
| `supervisorScope { ... }` | `await Coroutine.SupervisorScope(async scope => { ... }, onError, ct)` | Isolates child failures so healthy siblings keep running. |
| `scope.launch { ... }` | `Job job = scope.Launch(async ct => { ... });` | Starts a tracked child coroutine returning a cancellable/joinable `Job`. |
| `scope.async { ... }` | `Deferred<T> d = scope.Async(async ct => ...);` | Starts a tracked child returning a `Deferred<T>` (`await d` or `d.Cancel()`). |
| `job.cancel()` / `job.join()` | `job.Cancel();` / `await job.JoinAsync();` (or `await job;`) | Cancels or awaits an individual child without aborting siblings. |

### 2. Dispatchers & Context Switching

| Kotlin | C# (`KotlinStyleCoroutines`) | Behavior |
| :--- | :--- | :--- |
| `Dispatchers.Default` | `Dispatchers.Default` | Maps to `TaskScheduler.Default`. |
| `Dispatchers.Main` | `Dispatchers.Main` | Assign `SynchronizationContext.Current` at UI startup. |
| `Dispatchers.IO.limitedParallelism(n)` | `Dispatchers.LimitedParallelism(n)` | Caps concurrent execution via `ConcurrentExclusiveSchedulerPair`. |
| `newSingleThreadContext(...)` | `Dispatchers.SingleThread()` | Serializes work onto an exclusive single-thread scheduler. |
| `withContext(dispatcher) { ... }` | `await Coroutine.WithContext(scheduler, async scope => { ... }, ct)` | Switches scheduler/context for the scope and all launched children. |

### 3. Low-Level Continuation Adapters

| Kotlin | C# (`KotlinStyleCoroutines`) | Behavior |
| :--- | :--- | :--- |
| `suspendCancellableCoroutine { cont -> ... }` | `await Coroutine.SuspendCancellable<T>(cont => { ... }, ct)` | Zero-`Task` custom `INotifyCompletion` awaiter for callback APIs. |
| `cont.resume(value)` | `cont.Resume(value)` | Resumes execution inline (idempotent single-shot). |
| `cont.resumeWithException(ex)` | `cont.ResumeWithException(ex)` | Propagates exception to the `await` call site. |
| `cont.invokeOnCancellation { ... }` | `cont.InvokeOnCancellation(() => ...)` | Registers cleanup logic if the `CancellationToken` fires. |

### 4. Cold & Concurrent Flows (`IAsyncEnumerable<T>`)

| Kotlin | C# (`KotlinStyleCoroutines`) | Behavior |
| :--- | :--- | :--- |
| `flow { emit(x) }` | `async IAsyncEnumerable<T>` + `yield return x;` | Native C# cold stream with automatic backpressure & disposal. |
| `channelFlow { send(x) }` | `Coroutine.ChannelFlow<T>(async (scope, writer) => { ... })` | Concurrent fan-in from multiple `scope.Launch` writers. |
| `flow.flowOn(dispatcher)` | `flow.FlowOn(scheduler, bufferCapacity)` | Runs upstream enumeration on `scheduler` while streaming downstream. |
| `flow.launchIn(scope)` | `Job j = flow.LaunchIn(scope, async (x, ct) => { ... });` | Collects the flow inside a cancellable `Job` owned by `scope`. |

### 5. Hot Streams (`StateFlow<T>` & `SharedFlow<T>`)

| Kotlin | C# (`KotlinStyleCoroutines`) | Behavior |
| :--- | :--- | :--- |
| `MutableStateFlow(initial)` | `var state = new MutableStateFlow<T>(initial);` | Replay-1 state holder with equality deduplication and conflation. |
| `state.update { ... }` | `state.UpdateAndGet(prev => next)` / `CompareAndSet(...)` | Lock-protected atomic state transitions. |
| `MutableSharedFlow(replay, extra, overflow)` | `var events = new MutableSharedFlow<T>(replay, extra, overflow);` | Multi-subscriber broadcast stream with configurable `BufferOverflow`. |
| `flow.stateIn(scope, started, initial)` | `flow.StateIn(scope, SharingStarted.WhileSubscribed(), initial)` | Converts a cold stream into a hot `IStateFlow<T>`. |
| `flow.shareIn(scope, started, replay)` | `flow.ShareIn(scope, SharingStarted.Lazily, replay)` | Converts a cold stream into a hot `ISharedFlow<T>`. |
| `SharingStarted.Eagerly / Lazily / WhileSubscribed` | `SharingStarted.Eagerly` / `.Lazily` / `.WhileSubscribed(...)` | Controls upstream lifecycle based on `SubscriptionCount`. |

### 6. Flow Combinators, Time, & Recovery Operators

| Kotlin | C# (`KotlinStyleCoroutines`) | Behavior |
| :--- | :--- | :--- |
| `merge(f1, f2)` | `Coroutine.Merge(f1, f2)` | Interleaves elements concurrently as they arrive. |
| `combine(f1, f2) { a, b -> ... }` | `f1.CombineWith(f2, (a, b) => ...)` | Emits whenever either flow updates (once both have emitted). |
| `zip(f1, f2) { a, b -> ... }` | `f1.ZipWith(f2, (a, b) => ...)` | Pairs elements 1-to-1 in lockstep until either completes. |
| `flow.flatMapLatest { x -> subFlow }` | `flow.FlatMapLatest(x => subFlow)` | Cancels the previous inner stream whenever a new item arrives. |
| `flow.transformLatest { ... }` | `flow.TransformLatest(async (x, writer, ct) => { ... })` | Cancels in-flight transformation work on new upstream items. |
| `flow.debounce(timeout)` | `flow.Debounce(TimeSpan.FromMilliseconds(50), timeProvider)` | Emits after quiet window; flushes trailing item on completion. |
| `flow.sample(period)` | `flow.Sample(TimeSpan.FromMilliseconds(50), timeProvider)` | Periodically emits the latest unconsumed item. |
| `flow.catch { e -> emit(fallback) }` | `flow.Catch(ex => fallback)` | Recovers from upstream errors without swallowing cancellation. |
| `flow.retryWhen { e, attempt -> ... }` | `flow.RetryWhen(...)` / `flow.Retry(retries, predicate)` | Re-subscribes to failed upstream flows based on predicate. |

---

## End-to-End Example

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using KotlinStyleCoroutines;

public static class SearchViewModelExample
{
    public static async Task RunAsync(CancellationToken ct)
    {
        var query = new MutableStateFlow<string>("");

        await Coroutine.Scope(async scope =>
        {
            // Reactive search pipeline: debounce typing -> cancel stale searches -> recover on error -> share state
            IStateFlow<string> searchResults = query
                .Debounce(TimeSpan.FromMilliseconds(150))
                .FlatMapLatest(q => SearchBackendAsync(q))
                .Catch(ex => $"Error: {ex.Message}")
                .StateIn(scope, SharingStarted.WhileSubscribed(), initialValue: "Idle");

            // UI collector
            Job uiJob = searchResults.LaunchIn(scope, result =>
            {
                Console.WriteLine($"UI State -> {result}");
            });

            query.Value = "kot";
            await Task.Delay(40, ct);
            query.Value = "kotlin"; // Cancels "kot" search automatically

            await Task.Delay(300, ct);
            uiJob.Cancel();
            await uiJob.JoinAsync();
        }, ct);
    }

    private static async IAsyncEnumerable<string> SearchBackendAsync(
        string term,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(term))
        {
            yield return "Idle";
            yield break;
        }

        await Task.Delay(80, ct);
        yield return $"Results for '{term}'";
    }
}
```

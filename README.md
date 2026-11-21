# KotlinStyleCoroutines for .NET 11

[![.NET 11 CI](https://github.com/nabond251/KotlinStyleCoroutines/actions/workflows/dotnet.yml/badge.svg)](https://github.com/nabond251/KotlinStyleCoroutines/actions/workflows/dotnet.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A zero-dependency .NET 11 (C# 15) library bringing **Kotlin structured concurrency**, **custom cancellable continuations**, **Flow / StateFlow / SharedFlow reactive streams**, and **Jetpack Compose-style Blazor lifecycle bridges** to C# on top of `Task`, `IAsyncEnumerable<T>`, and `System.Threading.Channels`.

---

## Repository Structure

```text
KotlinStyleCoroutines/
├── .github/workflows/dotnet.yml             # .NET 11 CI build, test, sample run, and NuGet pack
├── Directory.Build.props                    # Centralized net11.0, Nullable, & analyzer settings
├── KotlinStyleCoroutines.slnx               # Modern XML .NET solution file
├── docs/
│   ├── ARCHITECTURE.md                      # Bottom-up 7-layer architecture & design invariants
│   ├── CONTRIBUTING.md                      # Onboarding, coding standards, & PR checklist
│   └── TESTING.md                           # Deterministic concurrency testing guide
├── src/
│   ├── KotlinStyleCoroutines/               # Core structured concurrency, Dispatchers, & Flow library
│   └── KotlinStyleCoroutines.Blazor/        # LifecycleComponentBase, CollectAsStateWithLifecycle, etc.
├── tests/
│   └── KotlinStyleCoroutines.Tests/         # xUnit test suite
└── samples/
    ├── CoroutineBasics/                     # Nested structured concurrency console sample
    ├── SearchDemo/                          # Debounce + FlatMapLatest + WhileSubscribed console sample
    └── BlazorSearchDemo/                    # Interactive Server Blazor app with ViewModel & StateFlow
```

---

## Quick Start

### Build & Run Tests

```bash
git clone https://github.com/nabond251/KotlinStyleCoroutines.git
cd KotlinStyleCoroutines

dotnet build KotlinStyleCoroutines.slnx
dotnet test KotlinStyleCoroutines.slnx
```

### Run the Sample Apps

```bash
# 1. Nested structured concurrency basics
dotnet run --project samples/CoroutineBasics/CoroutineBasics.csproj

# 2. Reactive Flow pipeline (Debounce + FlatMapLatest + StateIn)
dotnet run --project samples/SearchDemo/SearchDemo.csproj

# 3. Blazor Interactive Server demo (CollectAsStateWithLifecycle + LaunchedEffect + ProduceState)
dotnet run --project samples/BlazorSearchDemo/BlazorSearchDemo.csproj
```

---

## Documentation

* **[Architecture Guide (`docs/ARCHITECTURE.md`)](docs/ARCHITECTURE.md):** Bottom-up walkthrough of the 7 architectural layers (`Continuation<T>` → `CoroutineScope`/`Job` → `Dispatchers` → `ChannelFlow` → `SharedFlow`/`StateFlow` → Flow operators → Blazor lifecycle bindings).
* **[Contributing Guide (`docs/CONTRIBUTING.md`)](docs/CONTRIBUTING.md):** Development setup, analyzer rules (`TreatWarningsAsErrors`), `TimeProvider` guidelines, and PR workflow.
* **[Testing Guide (`docs/TESTING.md`)](docs/TESTING.md):** How to write deterministic, race-free tests for concurrent flows and hot state holders.

---

## API Cheat Sheet (Kotlin -> C#)

### 1. Structured Concurrency & Scopes

| Kotlin | C# (`KotlinStyleCoroutines`) | Behavior |
| :--- | :--- | :--- |
| `coroutineScope { ... }` | `await Coroutine.Scope(async scope => { ... }, ct)` | Waits for all child coroutines; cancels siblings if any child throws. |
| `supervisorScope { ... }` | `await Coroutine.SupervisorScope(async scope => { ... }, onError, ct)` | Isolates child failures so healthy siblings keep running. |
| `CoroutineScope(job)` | `var scope = Coroutine.CreateScope(parentToken)` | Creates a standalone scope for manual mount/unmount lifecycles. |
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

### 7. Jetpack Compose -> Blazor UI Bridges (`KotlinStyleCoroutines.Blazor`)

| Jetpack Compose | Blazor (`LifecycleComponentBase`) | Behavior |
| :--- | :--- | :--- |
| `collectAsStateWithLifecycle()` | `FlowState<T> s = CollectAsStateWithLifecycle(stateFlow);` | Subscribes on mount, triggers `StateHasChanged`, and pauses when browser tab is hidden. |
| `LaunchedEffect(key) { ... }` | `LaunchedEffect("id", key, async ct => { ... });` | Cancels and restarts a component-scoped `Job` whenever `key` changes. |
| `produceState(init, key) { ... }` | `FlowState<T> s = ProduceState("id", init, key, async ct => ...);` | Converts a keyed async producer into reactive UI state. |
| `rememberCoroutineScope()` | `_ = ComponentScope.Launch(async ct => { ... });` | Launches UI event coroutines bound to component disposal. |

---

## End-to-End Example

```csharp
using System.Runtime.CompilerServices;
using KotlinStyleCoroutines;

public static class SearchViewModelExample
{
    public static async Task RunAsync(CancellationToken ct)
    {
        var query = new MutableStateFlow<string>(string.Empty);

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
        [EnumeratorCancellation] CancellationToken ct = default)
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

## License

This project is licensed under the [MIT License](LICENSE).

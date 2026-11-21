# Architecture Guide

`KotlinStyleCoroutines` brings Kotlin-style structured concurrency, custom cancellable continuations, reactive `Flow` / `StateFlow` / `SharedFlow` streams, and Jetpack Compose-style Blazor UI lifecycle bridges to **.NET 11 (C# 15)**.

Rather than replacing .NET's `Task` or `IAsyncEnumerable<T>` ecosystems with incompatible runtime types, the library layers Kotlin's concurrency invariants directly onto `Task`, `CancellationTokenSource`, `TaskScheduler`, `IAsyncEnumerable<T>`, and `System.Threading.Channels`.

---

## Layered Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ Layer 7: Blazor UI Lifecycle Bridge (src/KotlinStyleCoroutines.Blazor)       │
│          LifecycleComponentBase, CollectAsStateWithLifecycle, LaunchedEffect │
├──────────────────────────────────────────────────────────────────────────────┤
│ Layer 6: Flow Operators (AsyncEnumerableFlowExtensions.cs, Coroutine.cs)     │
│          FlatMapLatest, Combine, Merge, Zip, Debounce, Sample, Catch, Retry  │
├──────────────────────────────────────────────────────────────────────────────┤
│ Layer 5: Hot Streams & Lifecycle (SharedFlow, StateFlow, SharingStarted)     │
│          Subscriber fan-out channels, replay ring buffer, SubscriptionCount  │
├──────────────────────────────────────────────────────────────────────────────┤
│ Layer 4: Cold & Concurrent Streams (ChannelFlow, FlowOn, LaunchIn)           │
│          Bridges parallel scope.Launch producers ──▶ IAsyncEnumerable<T>     │
├──────────────────────────────────────────────────────────────────────────────┤
│ Layer 3: Dispatchers & Context Switching (Dispatchers.cs, WithContext)       │
│          TaskScheduler.Current & SynchronizationContext propagation          │
├──────────────────────────────────────────────────────────────────────────────┤
│ Layer 2: Structured Concurrency (Job.cs, CoroutineScope.cs, Coroutine.cs)    │
│          Linked CancellationToken tree + ConcurrentBag<Task> draining        │
├──────────────────────────────────────────────────────────────────────────────┤
│ Layer 1: Compiler Continuation Primitive (Continuation.cs)                   │
│          Custom INotifyCompletion awaiter + lock-free atomic state           │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Layer-by-Layer Deep Dive

### Layer 1: Compiler Continuation Primitive (`Continuation.cs`)

C#'s `await` keyword is duck-typed: any type with a `GetAwaiter()` method returning an object implementing `INotifyCompletion` (`IsCompleted`, `OnCompleted(Action)`, and `GetResult()`) can be awaited without allocating a `Task` or `TaskCompletionSource<T>`.

* **`Suspendable<T>` & `Continuation<T>`:** `Coroutine.SuspendCancellable<T>` creates a `Continuation<T>`, registers an optional `CancellationToken`, invokes the user callback, and returns a `readonly struct Suspendable<T>`.
* **Lock-Free Suspension vs. Completion Race:** An external callback (such as an event handler or socket completion) can invoke `Resume(value)` either *before* or *after* the compiler calls `OnCompleted(continuation)`. `Continuation<T>` resolves this race without locks using an integer `_state` (`0 = Pending`, `1 = Completed`, `2 = Canceled`) and `Interlocked.CompareExchange` on `_continuation`:
  * **Compiler suspends first:** `OnCompleted` stores `continuation` in `_continuation`. When `Resume` runs later, `Complete()` extracts the delegate via `Interlocked.Exchange` and invokes it inline.
  * **Callback completes first:** `Complete()` swaps a no-op sentinel (`static () => { }`) into `_continuation`. When `OnCompleted` subsequently runs, `CompareExchange` sees the non-null sentinel and executes `continuation()` immediately without suspending.
* **Memory Leak Prevention:** `Complete()` disposes the `CancellationTokenRegistration` so completed continuations are never retained by long-lived parent tokens.

---

### Layer 2: Structured Concurrency (`Job.cs`, `CoroutineScope.cs`, `Coroutine.cs`)

Standard C# `Task` instances are unstructured ("fire-and-forget" by default): launching a `Task` inside a method does not tie its lifetime, cancellation, or unhandled exceptions to the enclosing block. Layer 2 enforces a strict parent-child tree.

* **Linked Cancellation Tree (`Job` and `Deferred<T>`):**
  * Each `CoroutineScope` owns a root `CancellationTokenSource` (`_cts`) linked to the caller's `CancellationToken`.
  * Calling `scope.Launch(...)` or `scope.Async<T>(...)` creates a child `CancellationTokenSource` via `CancellationTokenSource.CreateLinkedTokenSource(_cts.Token)` and wraps it with the child `Task` in a `Job` or `Deferred<T>`.
  * Canceling the parent scope cascades downward to all children; calling `job.Cancel()` cancels only that specific child and its descendants.
* **Fail-Fast vs. Supervisor Error Propagation (`RunChildAsync`):**
  * Every child `Task` is recorded in `_children` (`ConcurrentBag<Task>`).
  * **`Coroutine.Scope` (Fail-Fast):** If any child throws an unhandled exception (excluding `OperationCanceledException` when `childCts.IsCancellationRequested` is `true`), `RunChildAsync` captures the first failure with `ExceptionDispatchInfo` and calls `await _cts.CancelAsync()`, immediately canceling all sibling jobs.
  * **`Coroutine.SupervisorScope` (Fault Isolation):** Sets `_isSupervisor = true`. Child failures invoke `onUnhandledChildException` (for `Launch`) or surface only to callers awaiting that specific `Deferred<T>` (for `Async<T>`), leaving siblings untouched.
* **Deterministic Cleanup (`DisposeAsync`):**
  * `Coroutine.Scope` executes inside `await using var scope = ...`.
  * `DisposeAsync()` drains `_children` by awaiting every child `Task`, disposes `_cts`, and rethrows `_firstException`. No child coroutine can outlive its parent scope.

---

### Layer 3: Dispatchers & Context Switching (`Dispatchers.cs`, `Coroutine.WithContext`)

When `SynchronizationContext.Current` is `null`, C# `await` continuations resume on `TaskScheduler.Current` unless stripped by `.ConfigureAwait(false)`.

* **Scheduler Factories (`Dispatchers.cs`):**
  * `Dispatchers.Default` maps to `TaskScheduler.Default`.
  * `Dispatchers.Main` holds a configurable UI `SynchronizationContext`.
  * `Dispatchers.LimitedParallelism(n)` and `Dispatchers.SingleThread()` wrap `ConcurrentExclusiveSchedulerPair` over `TaskScheduler.Default` to cap concurrency without dedicating OS threads.
* **Context Pinning (`WithContext`):**
  * `Task.Factory.StartNew(() => Scope(block, ct), ct, TaskCreationOptions.DenyChildAttach, scheduler).Unwrap()` starts a `CoroutineScope` on `scheduler`.
  * Because `TaskScheduler.Current` flows into child `scope.Launch` calls and across standard `await` points (that do not call `.ConfigureAwait(false)`), the entire child tree stays pinned to `scheduler` until `WithContext` exits.

---

### Layer 4: Cold & Concurrent Streams (`ChannelFlow`, `FlowOn`, `LaunchIn`)

Native C# `async IAsyncEnumerable<T>` already provides pull-based cold streams with automatic backpressure (producers pause at `yield return` until consumers call `MoveNextAsync()`).

* **Concurrent Fan-In (`Coroutine.ChannelFlow<T>`):** C# forbids `yield return` inside nested lambdas or parallel tasks. `ChannelFlow` creates a bounded `Channel<T>`, runs a `Coroutine.Scope` in the background where multiple `scope.Launch` workers write to `ChannelWriter<T>`, and yields items sequentially from `channel.Reader.ReadAllAsync()`.
  * A `.ContinueWith(...)` on the `Scope` task calls `channel.Writer.TryComplete()` only after `DisposeAsync()` finishes awaiting **all** launched workers.
  * If a consumer breaks out of `await foreach` early, the `finally` block cancels `linkedCts`, immediately tearing down all upstream workers.
* **Dispatcher Boundary (`FlowOn`) & Terminal Collection (`LaunchIn`):**
  * `FlowOn(scheduler, bufferCapacity)` enumerates upstream on `scheduler` via `WithContext` and streams items downstream through a bounded channel.
  * `LaunchIn(scope, onEach)` collects an `IAsyncEnumerable<T>` inside a cancellable `Job` owned by `scope`.

---

### Layer 5: Hot Streams & Lifecycles (`SharedFlow.cs`, `StateFlow.cs`, `SharingStarted.cs`)

Unlike cold `IAsyncEnumerable<T>` streams, hot flows exist independently of collectors and broadcast values to `0..N` concurrent subscribers.

* **Multi-Subscriber Broadcast Hub (`MutableSharedFlow<T>`):**
  * Maintains a lock-protected `List<Subscriber>` (each backed by a `Channel<T>` configured for `BufferOverflow.Suspend`, `DropOldest`, or `DropLatest`) and a `Queue<T>` replay ring buffer of capacity `replay`.
  * Subscribing (`GetAsyncEnumerator`) atomically snapshots `_replayBuffer`, registers a new `Subscriber` channel, and increments `_subscriptionCount`. Exiting `await foreach` unregisters the subscriber in `finally` and decrements `_subscriptionCount`.
* **Conflated State Holder (`MutableStateFlow<T>`):**
  * Protects a single `Value` behind a `Lock`, suppresses duplicate emissions using `EqualityComparer<T>.Default`, provides atomic `CompareAndSet` / `UpdateAndGet` / `GetAndUpdate` transitions, and immediately replays the latest `Value` to new collectors.
* **Reactive Lifecycle Control (`SharingStarted` & `.ShareIn` / `.StateIn`):**
  * `SubscriptionCount` on `ISharedFlow<T>` is itself an `IStateFlow<int>`.
  * `SharingStarted.WhileSubscribed` enumerates `SubscriptionCount` and emits `SharingCommand.Start` when `count > 0` and `SharingCommand.Stop` (after an optional `stopTimeout`) when `count == 0`. `LaunchSharing` listens to these commands inside `scope` to start or cancel the upstream producer `Job`.

---

### Layer 6: Reactive Flow Operators (`AsyncEnumerableFlowExtensions.cs`, `Coroutine.cs`)

High-level operators compose Layers 2–5:

* **Latest-Switching (`TransformLatest`, `FlatMapLatest`):** Uses `ChannelFlow` to read upstream items; on each new emission, it cancels and joins the active inner `Job` (`activeJob.Cancel(); await activeJob.JoinAsync();`) before launching the new inner work.
* **Combinators (`Merge`, `Combine`, `Zip`):** `Merge` runs parallel `scope.Launch` workers into a `ChannelFlow`; `Combine` tracks latest values under a lock and signals an unbounded output channel; `Zip` advances two `IAsyncEnumerator` instances in lockstep.
* **Time-Based (`Debounce`, `Sample`):** Uses `TimeProvider` (supporting deterministic virtual-time testing) and generation counters inside `ChannelFlow` to suppress rapid bursts or emit periodic snapshots.
* **Error Recovery (`Catch`, `RetryWhen`, `Retry`):** Manually drives `IAsyncEnumerator<T>.MoveNextAsync()` inside `try/catch` (yielding outside `try` to satisfy C# iterator constraints) while never swallowing downstream `cancellationToken` cancellations.

---

### Layer 7: Blazor UI Lifecycle Bridge (`src/KotlinStyleCoroutines.Blazor`)

* **`LifecycleComponentBase`:** Creates `ComponentScope = Coroutine.CreateScope(_lifecycleCts.Token)` in `OnInitialized` and cancels/drains it in `DisposeAsync`.
* **`CollectAsStateWithLifecycle`:** Subscribes to an `IStateFlow<T>` or `IAsyncEnumerable<T>`, updates `FlowState<T>.Value` and triggers `InvokeAsync(StateHasChanged)` on updates, and imports `wwwroot/visibility.js` (`document.visibilitychange`) to pause/resume collection when the browser tab is hidden.
* **`LaunchedEffect` & `ProduceState`:** Track parameter keys across `OnParametersSet` calls, canceling and restarting child `Job` instances whenever keys change.

---

## Key Invariants Contributors Must Preserve

1. **Never Swallow Caller Cancellation:** Whenever catching `OperationCanceledException`, check `when (!cancellationToken.IsCancellationRequested)` (or `when (childCts.IsCancellationRequested)` for child-scoped cancellation) so downstream/parent cancellation propagates cleanly instead of being treated as a retryable or recoverable error.
2. **Preserve `TaskScheduler.Current` Inside `WithContext`:** Avoid `.ConfigureAwait(false)` on internal awaits where custom `TaskScheduler` affinity must be preserved across suspension points.
3. **Avoid `yield return` Inside `try/catch` Blocks:** C# prohibits `yield return` inside a `try` block that has a `catch` clause (CS1626). Always capture items/exceptions into local variables inside `try/catch` and `yield return` outside.
4. **Dispose Linked `CancellationTokenSource` and Registrations:** Every child `CancellationTokenSource` created in `CoroutineScope` or `ChannelFlow` must be disposed in a `finally` block to prevent memory retention on long-lived parent tokens.

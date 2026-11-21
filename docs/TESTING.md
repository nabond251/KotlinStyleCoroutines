# Testing Guide (`KotlinStyleCoroutines`)

This guide explains how the xUnit test suite is organized, how to write deterministic concurrency and Flow tests without flaky `Task.Delay` races, and how to run tests locally and in CI.

## Running the Test Suite

From the repository root:

```bash
# Run all tests
dotnet test KotlinStyleCoroutines.slnx

# Run in Release mode (matches CI)
dotnet test KotlinStyleCoroutines.slnx --configuration Release --verbosity normal

# Filter by test class or method name
dotnet test --filter "FullyQualifiedName~HotFlowsTests"
```

---

## Test Suite Organization

All unit tests live under `tests/KotlinStyleCoroutines.Tests/` and mirror the architectural layers of the core library:

| Test File | What It Verifies |
| :--- | :--- |
| **`StructuredConcurrencyTests.cs`** | `SuspendCancellable` inline resumption and cancellation callbacks; `Coroutine.Scope` fail-fast sibling cancellation; `Coroutine.SupervisorScope` isolated child failure handling; individual `Job.Cancel()` and `Job.JoinAsync()`. |
| **`HotFlowsTests.cs`** | `MutableStateFlow` replay-1 semantics, equality deduplication, and slow-consumer conflation; `MutableSharedFlow` multi-subscriber broadcasting and replay buffer; `ShareIn` with `SharingStarted.WhileSubscribed` start/stop/restart lifecycle transitions. |
| **`FlowOperatorsTests.cs`** | `CombineWith`, `Coroutine.Merge`, and `ZipWith` stream composition; `FlatMapLatest` cancellation of in-flight inner streams; `Debounce` and `Sample` time-based filtering; `Retry` and `Catch` upstream fault recovery. |

---

## Writing Deterministic Concurrency Tests

Testing asynchronous state machines, cancellation propagation, and hot broadcast flows can easily introduce timing races if tests rely on arbitrary wall-clock delays. Follow these four patterns used across `tests/KotlinStyleCoroutines.Tests/`:

### 1. Synchronize Readiness with `SubscriptionCount` or `TaskCompletionSource`

When testing `SharedFlow`, `StateFlow`, or `Combine`, never assume a background `scope.Launch` collector has subscribed on the very next line:

```csharp
// Wait until the hot flow registers the active subscriber before emitting
while (shared.SubscriptionCount.Value < 2)
{
    await Task.Delay(5);
}
```

For multi-step state assertions (such as verifying intermediate `CombineWith` emissions before overwriting a `MutableStateFlow`), gate each mutation behind a `TaskCompletionSource<bool>` signaled by the collector:

```csharp
var gotInitial = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

_ = combined.LaunchIn(scope, async (value, _) =>
{
    observed.Add(value);
    if (value == "Alice:Viewer") gotInitial.TrySetResult(true);
    await Task.CompletedTask;
});

await gotInitial.Task.WaitAsync(TimeSpan.FromSeconds(2));
role.Value = "Admin";
```

### 2. Exit `await foreach` Loops Cleanly Inside `scope.Launch`

When a test collector inside `scope.Launch(async ct => ...)` finishes reading the expected number of items, exit the loop with `break;` rather than canceling an unrelated `CancellationTokenSource`:

```csharp
var subJob = scope.Launch(async ct =>
{
    await foreach (var item in hot.WithCancellation(ct))
    {
        collected.Add(item);
        if (collected.Count >= 2)
            break; // Disposes enumerator cleanly and decrements SubscriptionCount
    }
});

await subJob.JoinAsync();
```

*Why:* `CoroutineScope.RunChildAsync` checks `when (childCts.IsCancellationRequested)`. Throwing an `OperationCanceledException` from an external token inside a child `Job` looks like an unhandled failure to the parent scope and will cancel all sibling jobs.

### 3. Use `TimeProvider` for Time-Based Operators

`Debounce` and `Sample` accept an optional `TimeProvider`. Pass a virtual/fake `TimeProvider` (such as `FakeTimeProvider` from `Microsoft.Extensions.TimeProvider.Testing`) or tight controlled intervals so tests execute in milliseconds without depending on OS thread-pool timer jitter.

### 4. Respect Repo Analyzer Rules in Test Projects

`Directory.Build.props` applies `AnalysisLevel=latest-recommended` and `TreatWarningsAsErrors=true` to test projects as well:

* **No underscores in test method names (`CA1707`):** Use PascalCase names (`StateFlowReplaysCurrentValueAndConflates`) instead of `Given_When_Then` underscores.
* **Extract repeated array assertions (`CA1861`):** Declare expected collections as `private static readonly int[] ExpectedItems = [1, 2, 3];` at the class level rather than passing inline array literals repeatedly to `Assert.Equal`.
* **Discard intentionally unawaited `Job` handles (`CS4014`):** Use `_ = scope.Launch(...)` when you do not intend to await or cancel the returned `Job`.

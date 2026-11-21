# Project Handoff: KotlinStyleCoroutines (.NET 11)

## 1. Current State Summary

* **Repository:** `https://github.com` (primary branch: `master`)
* **Target Framework:** `.NET 11.0` (`C# 15`), zero external runtime dependencies in the core library.
* **Build & Quality Status:** Compiles cleanly with strict warnings-as-errors, `latest-recommended` .NET analyzers, and generated XML documentation across packable libraries.
* **Test Status:** All xUnit core tests and bUnit (`2.11.3`) Blazor component tests pass.
* **CI/CD Status:** Automated GitHub Actions build, test, sample execution, and NuGet packaging workflow triggered on push/PR to `master`.

---

## 2. Solution & Repository Layout

The solution is organized under `/src`, `/tests`, `/samples`, and `/docs`:
* **`src/KotlinStyleCoroutines/`:** Implements a 7-layer concurrency model ranging from low-level compiler continuations (`Continuation.cs`), structured scopes (`CoroutineScope.cs`, `Job.cs`), dispatchers (`Dispatchers.cs`), and channels/select (`Channel.cs`, `Select.cs`), to hot streams (`SharedFlow.cs`, `StateFlow.cs`), flow operators, and ambient cancellation contexts.
* **`src/KotlinStyleCoroutines.Blazor/`:** Jetpack Compose-style Blazor UI lifecycle bridge (`LifecycleComponentBase.cs`, `FlowState.cs`) with CSP-safe visibility observers.
* **`tests/` & `samples/`:** Comprehensive test suites and runnable console/Blazor demo apps (`CoroutineBasics`, `SearchDemo`, `BlazorSearchDemo`).

---

## 3. Architectural Overview (Bottom-Up)

1. **Layer 1 — Compiler Continuation Primitive:** Custom duck-typed `INotifyCompletion` / `Suspendable<T>` avoid `TaskCompletionSource<T>` overhead via lock-free state transitions.
2. **Layer 2 — Structured Concurrency & Hybrid Ambient Context:** Scopes link child cancellation tokens and track background tasks; `Coroutine.CurrentToken` (`AsyncLocal<CancellationToken>`) propagates cancellation invisibly while preserving explicit BCL interop.
3. **Layer 3 — Dispatchers:** Pins `TaskScheduler` or `UI SynchronizationContext` across scoped execution trees using `WithContext`.
4. **Layer 4 — Structured Channels & `Select`:** Automatic channel cleanup upon scope exit and atomic race-condition selection (`OnReceive`, `OnAwait`, `OnTimeout`) via `Interlocked` winner flags and linked cancellation tokens.
5. **Layer 5 — Hot Streams:** Replay-buffered `MutableSharedFlow<T>`, conflated `MutableStateFlow<T>`, and `SharingStarted` lifecycle policies.
6. **Layer 6 — Flow Operators:** Common reactive operators (`FlatMapLatest`, `Combine`, `Debounce`, `Sample`, `Catch`, `RetryWhen`).
7. **Layer 7 — Blazor Lifecycle Bridge:** Binds `CoroutineScope` to `IAsyncDisposable`, bridges reactive state to `StateHasChanged`, and handles tab visibility pause/resume events.

---

## 4. Build, Test, & CI/CD Notes

* **Local Build & Test:** Run `dotnet build KotlinStyleCoroutines.slnx` and `dotnet test KotlinStyleCoroutines.slnx`.
* **API Documentation & Packaging:** Public/protected APIs require XML doc comments (`CS1591` enforced).
* **Blazor & Security:** JSRuntime/JSObjectReference calls are wrapped against prerendering/disconnection errors; dependencies are pinned to resolve CVE-2026-118749 (`NU1902`).
* **CI Samples:** Console demos execute safely with timeout controls in the GitHub Actions workflow.

---

## 5. Critical Engineering Invariants

1. **Child Cancellation Inside Scopes:** Do not manually cancel unlinked cancellation tokens inside `scope.Launch` callbacks and let `OperationCanceledException` escape, as unlinked cancellations trigger parent scope failure.
2. **Preserve Caller Cancellation:** Filter out caller-initiated cancellations (`catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)`) inside operators.
3. **Bounded Background Tasks:** Explicitly cancel and join background loops inside channel flows (e.g., `Sample` tickers) before scope disposal.
4. **TaskScheduler Affinity:** Avoid `.ConfigureAwait(false)` on awaits where custom `TaskScheduler` context must flow.

---

## 6. Recommended Next Steps for Future Maintainers

* **Expand `SelectBuilder<TResult>`:** Add `OnSend` clauses for writable bounded channels.
* **Blazor Sample Tests:** Add bUnit coverage for `BlazorSearchDemo`.
* **Automated NuGet Release:** Configure release tags and `NUGET_API_KEY` secrets to push artifacts to NuGet.org.

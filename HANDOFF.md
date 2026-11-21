Project Handoff: KotlinStyleCoroutines (.NET 11)


1. Current State Summary

• Repository: https://github.com (primary branch: master)
• Target Framework: .NET 11.0 (C# 15), zero external runtime dependencies in the core library.
• Build & Quality Status: Compiles cleanly with <TreatWarningsAsErrors>true</TreatWarningsAsErrors>, latest-recommended .NET analyzers, and <GenerateDocumentationFile>true</GenerateDocumentationFile> enabled across all packable libraries.
• Test Status: All xUnit tests in KotlinStyleCoroutines.Tests and bUnit (2.11.3) component tests in KotlinStyleCoroutines.Blazor.Tests pass.
• CI/CD Status: .github/workflows/dotnet.yml triggers on push/pull_request to master and workflow_dispatch, building the solution, running all tests, executing console samples, and packing NuGet artifacts for both libraries.

2. Solution & Repository Layout

Show Code
Download diagram SVG

3. Architectural Overview (Bottom-Up)

1. Layer 1 — Compiler Continuation Primitive (Continuation.cs): Uses duck-typed INotifyCompletion (Suspendable<T> / Continuation<T>) with lock-free Interlocked.CompareExchange state transitions to adapt callback APIs without allocating TaskCompletionSource<T>.
2. Layer 2 — Structured Concurrency & Hybrid Ambient Context (Job.cs, CoroutineScope.cs, Coroutine.AmbientContext.cs):
	• Every CoroutineScope links child CancellationTokenSource instances to its root token and tracks child tasks in a ConcurrentBag<Task> drained in DisposeAsync().
	• Coroutine.CurrentToken (AsyncLocal<CancellationToken>) automatically links nested Coroutine.Scope / ChannelFlow / Select calls when cancellationToken == default and powers Coroutine.Delay, Coroutine.EnsureActive, and Coroutine.NonCancellable, while keeping explicit CancellationToken parameters for .NET BCL interop (HttpClient, Stream, EF Core).
3. Layer 3 — Dispatchers (Dispatchers.cs, Coroutine.WithContext): Pins TaskScheduler.Current (via ConcurrentExclusiveSchedulerPair) or UI SynchronizationContext across a scoped async execution tree.
4. Layer 4 — Structured Channels & Select (ChannelFlow, ChannelBuilders.cs, Select.cs):
	• ChannelFlow<T>, scope.Produce<T>, and scope.Actor<T> wrap System.Threading.Channels inside structured scopes so channels complete automatically when producers/actors finish or fail.
	• Coroutine.Select<TResult> races OnReceive, OnReceiveCatching, OnAwait, OnJoin, and OnTimeout clauses using an atomic winner flag (Interlocked.CompareExchange) and linked raceCts so losing clauses cancel immediately without consuming items from losing channels.
5. Layer 5 — Hot Streams (SharedFlow.cs, StateFlow.cs, SharingStarted.cs): Provides replay-buffered multi-subscriber channels (MutableSharedFlow<T>), conflated atomic state holders (MutableStateFlow<T>), and reactive upstream lifecycle management (SharingStarted.WhileSubscribed observing SubscriptionCount).
6. Layer 6 — Flow Operators (AsyncEnumerableFlowExtensions.cs): Composes Layers 2–5 for FlatMapLatest, TransformLatest, CombineWith, Merge, ZipWith, Debounce, Sample, Catch, and RetryWhen.
7. Layer 7 — Blazor Lifecycle Bridge (KotlinStyleCoroutines.Blazor): LifecycleComponentBase binds a CoroutineScope to Blazor's IAsyncDisposable, re-renders via InvokeAsync(StateHasChanged) in CollectAsStateWithLifecycle, pauses/resumes collection on tab visibility changes, and restarts keyed LaunchedEffect / ProduceState coroutines across parameter updates.

4. Build, Test, & CI/CD Notes

• Local Build & Test:bash
dotnet build KotlinStyleCoroutines.slnx
dotnet test KotlinStyleCoroutines.slnx
Use code with caution.
• NuGet Packaging: Both KotlinStyleCoroutines.csproj and KotlinStyleCoroutines.Blazor.csproj include NuGet metadata and <GenerateDocumentationFile>true</GenerateDocumentationFile>. Any new public or protected APIs must include XML doc comments or the build will fail on CS1591.
• Blazor Analyzer (BL0016): All IJSRuntime / IJSObjectReference calls in LifecycleComponentBase must stay wrapped in try/catch blocks to handle prerendering and circuit disconnection cleanly.
• Security Baseline: KotlinStyleCoroutines.Blazor.Tests.csproj explicitly pins Microsoft.AspNetCore.Components.Web and Microsoft.Extensions.Validation to 11.0.0 GA to avoid NU1902 (CVE-2026-118749 affecting 11.0.0-rc.1–rc.3).
• CI Console Sample Execution: samples/SearchDemo runs continuously until Ctrl+C. In .github/workflows/dotnet.yml, it is executed via timeout -s INT 3s dotnet run ... < /dev/null (without --foreground, so SIGINT reaches the child process group) and accepts exit codes 0, 124, 130, and 137.

5. Critical Engineering Invariants

1. Child Cancellation Inside CoroutineScope: Never cancel an unlinked CancellationTokenSource inside a scope.Launch callback and let its OperationCanceledException escape; RunChildAsync checks when (childCts.IsCancellationRequested) and will treat an unlinked cancellation as an unhandled child failure, canceling the entire parent scope. Exit loops via break; or cancel the child's Job.
2. Never Swallow Caller Cancellation: Always filter catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) in operators (Catch, RetryWhen, Select) so downstream cancellation propagates immediately.
3. Background Child Tasks Must Be Bounded: Any helper scope.Launch loop inside a ChannelFlow (such as the periodic ticker in Sample) must be explicitly canceled and joined (tickerJob.Cancel(); await tickerJob.JoinAsync();) when upstream completes, or CoroutineScope.DisposeAsync() will wait indefinitely.
4. Preserve TaskScheduler.Current Inside WithContext: Avoid .ConfigureAwait(false) on awaits where custom TaskScheduler affinity is expected to flow across suspension points.

6. Recommended Next Steps for Future Maintainers

• Sync Markdown Docs with Latest Additions: Update README.md (API Cheat Sheet) and docs/ARCHITECTURE.md (Layers 2 and 4) to document scope.Produce, scope.Actor, ChannelCapacity, Coroutine.Select, and Coroutine.CurrentToken / Coroutine.NonCancellable, and list tests/KotlinStyleCoroutines.Blazor.Tests in the repository tree.
• Expand SelectBuilder<TResult> (Optional): Add OnSend<T>(ChannelWriter<T>, T item, ...) clauses to support selecting across writable bounded channels alongside OnReceive.
• Blazor Sample Test Coverage (Optional): Add bUnit tests for samples/BlazorSearchDemo (SearchViewModel + Home.razor) or a CI smoke check verifying the web host starts cleanly.
• Publish NuGet Packages: Configure a NUGET_API_KEY secret and tag-triggered release job in .github/workflows/dotnet.yml to push ./artifacts/*.nupkg to NuGet.org.

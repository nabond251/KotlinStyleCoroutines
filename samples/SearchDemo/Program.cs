using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using KotlinStyleCoroutines;

using var appCts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    appCts.Cancel();
};

Console.WriteLine("=== KotlinStyleCoroutines .NET 11 Sample ===");

var query = new MutableStateFlow<string>("");
var events = new MutableSharedFlow<string>(replay: 1, extraBufferCapacity: 8);

await Coroutine.Scope(async scope =>
{
    // 1. Background telemetry collector (SharedFlow)
    Job telemetryJob = events.LaunchIn(scope, evt =>
    {
        Console.WriteLine($"[Telemetry] {evt}");
    });

    // 2. Reactive search pipeline:
    //    StateFlow -> Debounce -> FlatMapLatest -> FlowOn(LimitedParallelism) -> Retry -> Catch -> StateIn
    var searchDispatcher = Dispatchers.LimitedParallelism(maxDegreeOfParallelism: 2);

    IStateFlow<string> searchResults = query
        .Debounce(TimeSpan.FromMilliseconds(120))
        .FlatMapLatest(term => SearchBackendAsync(term, events))
        .FlowOn(searchDispatcher)
        .Retry(retries: 2, predicate: ex => ValueTask.FromResult(ex is TimeoutException))
        .Catch(ex => $"[Error] {ex.Message}")
        .StateIn(scope, SharingStarted.WhileSubscribed(), initialValue: "Idle");

    // 3. UI state collector (StateFlow)
    Job uiJob = searchResults.LaunchIn(scope, state =>
    {
        Console.WriteLine($"[UI State]  {state}");
    });

    // Simulate rapid user typing ("kot" gets debounced out, "kotlin" starts search, then "kotlin coroutines" cancels it)
    await Task.Delay(50, scope.Token);
    query.Value = "kot";
    await Task.Delay(40, scope.Token);
    query.Value = "kotlin";

    // Wait long enough for debounce (120ms) to trigger "kotlin" search, then type again mid-flight to cancel it
    await Task.Delay(160, scope.Token);
    query.Value = "kotlin coroutines";

    // Wait for the final search to complete and emit
    await Task.Delay(350, scope.Token);

    // 4. Demonstrate SuspendCancellable callback bridge
    string legacyResult = await Coroutine.SuspendCancellable<string>(cont =>
    {
        _ = Task.Run(async () =>
        {
            await Task.Delay(40);
            cont.Resume("Callback completed cleanly");
        });
    }, scope.Token);

    Console.WriteLine($"[Callback]  {legacyResult}");

    // Cleanly cancel long-lived collectors before exiting the scope
    uiJob.Cancel();
    telemetryJob.Cancel();
    await Task.WhenAll(uiJob.JoinAsync(), telemetryJob.JoinAsync());
}, appCts.Token);

Console.WriteLine("=== Done ===");

static async IAsyncEnumerable<string> SearchBackendAsync(
    string term,
    MutableSharedFlow<string> telemetry,
    [EnumeratorCancellation] CancellationToken ct = default)
{
    if (string.IsNullOrWhiteSpace(term))
    {
        yield return "Idle";
        yield break;
    }

    await telemetry.EmitAsync($"Starting search for '{term}' on thread {Environment.CurrentManagedThreadId}", ct);

    try
    {
        await Task.Delay(120, ct);
        yield return $"3 results found for '{term}'";
        await telemetry.EmitAsync($"Completed search for '{term}'", ct);
    }
    finally
    {
        if (ct.IsCancellationRequested)
        {
            // Use CancellationToken.None for cleanup telemetry since `ct` is already canceled
            telemetry.TryEmit($"Canceled stale search for '{term}'");
        }
    }
}

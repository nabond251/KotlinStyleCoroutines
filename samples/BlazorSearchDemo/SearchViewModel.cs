using System.Runtime.CompilerServices;
using KotlinStyleCoroutines;

namespace BlazorSearchDemo;

public sealed record SearchUiState(
    bool IsLoading,
    string Query,
    IReadOnlyList<string> Results,
    string? ErrorMessage = null)
{
    public static SearchUiState Idle { get; } = new(false, string.Empty, []);
}

public sealed class SearchViewModel : IAsyncDisposable
{
    private static readonly string[] Catalog =
    [
        "Kotlin Coroutines: Structured Concurrency",
        "Kotlin Flow: Cold vs Hot Streams",
        "StateFlow & SharedFlow in Compose",
        "Blazor Interactive Server with .NET 11",
        "C# 15 Async Enumerables & Channels",
        "FlatMapLatest & Debounce Patterns",
        "SupervisorScope Fault Isolation"
    ];

    private readonly CancellationTokenSource _vmCts = new();
    private readonly CoroutineScope _vmScope;
    private readonly MutableStateFlow<string> _query = new(string.Empty);
    private readonly MutableSharedFlow<string> _toasts = new(replay: 0, extraBufferCapacity: 16);

    public IStateFlow<string> Query => _query;
    public IStateFlow<SearchUiState> UiState { get; }
    public ISharedFlow<string> Toasts => _toasts;

    public SearchViewModel()
    {
        _vmScope = Coroutine.CreateScope(_vmCts.Token);

        UiState = _query
            .Debounce(TimeSpan.FromMilliseconds(250))
            .FlatMapLatest(q => SearchCatalogAsync(q))
            .Catch(ex => new SearchUiState(false, _query.Value, [], ex.Message))
            .StateIn(
                _vmScope,
                SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.FromSeconds(5)),
                SearchUiState.Idle);
    }

    public void OnQueryChanged(string value) => _query.Value = value;

    public async Task RefreshAsync(CancellationToken ct = default)
    {
        var current = _query.Value;
        await _toasts.EmitAsync(
            string.IsNullOrWhiteSpace(current)
                ? "Refreshed catalog index."
                : $"Refreshed results for '{current}'.",
            ct);
    }

    public async Task<string> FetchTrendingTipAsync(string category, CancellationToken ct = default)
    {
        await Task.Delay(150, ct);
        return category switch
        {
            "Flow" => "Tip: Combine .Debounce() + .FlatMapLatest() + .StateIn(WhileSubscribed()) to cancel stale queries automatically.",
            "Blazor" => "Tip: Switch browser tabs while a search is active—CollectAsStateWithLifecycle pauses collection until you return.",
            _ => "Tip: Try typing 'kotlin', 'flow', or 'blazor' rapidly to see FlatMapLatest cancel stale searches."
        };
    }

    private static async IAsyncEnumerable<SearchUiState> SearchCatalogAsync(
        string term,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(term))
        {
            yield return SearchUiState.Idle;
            yield break;
        }

        yield return new SearchUiState(true, term, []);

        // Simulate backend latency; canceled automatically by FlatMapLatest if user keeps typing
        await Task.Delay(350, ct);

        if (term.Equals("error", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Simulated backend error (type something else to recover).");

        var matches = Catalog
            .Where(item => item.Contains(term, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        yield return new SearchUiState(false, term, matches);
    }

    public async ValueTask DisposeAsync()
    {
        await _vmCts.CancelAsync();
        await _vmScope.DisposeAsync();
        _vmCts.Dispose();
    }
}

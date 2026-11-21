using System.Runtime.CompilerServices;
using KotlinStyleCoroutines;

namespace BlazorSearchDemo;

public sealed record SearchUiState(
    string Query,
    bool IsLoading,
    IReadOnlyList<string> Results,
    string? ErrorMessage = null);

public sealed class SearchViewModel : IAsyncDisposable
{
    private static readonly string[] Catalog =
    [
        "CoroutineScope & Structured Concurrency",
        "SupervisorScope Fault Isolation",
        "SuspendCancellable Custom Continuations",
        "ChannelFlow Concurrent Producers",
        "MutableStateFlow & Conflated UI State",
        "MutableSharedFlow Event Broadcasting",
        "SharingStarted.WhileSubscribed Lifecycle",
        "FlatMapLatest Search Cancellation",
        "Debounce & Sample Time Operators",
        "CollectAsStateWithLifecycle for Blazor"
    ];

    private readonly CoroutineScope _viewModelScope = Coroutine.CreateScope();
    private readonly MutableStateFlow<string> _query = new(string.Empty);
    private readonly MutableSharedFlow<string> _toasts = new(replay: 0, extraBufferCapacity: 8);

    public IStateFlow<string> Query => _query;
    public ISharedFlow<string> Toasts => _toasts;
    public IStateFlow<SearchUiState> UiState { get; }

    public SearchViewModel()
    {
        UiState = _query
            .Debounce(TimeSpan.FromMilliseconds(250))
            .FlatMapLatest(q => SearchCatalogAsync(q))
            .Catch(ex => new SearchUiState(_query.Value, IsLoading: false, [], $"Search failed: {ex.Message}"))
            .StateIn(
                _viewModelScope,
                SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.FromSeconds(5)),
                new SearchUiState(string.Empty, IsLoading: false, Catalog));
    }

    public void UpdateQuery(string query) => _query.Value = query;

    public async Task RefreshAsync(CancellationToken ct = default)
    {
        var current = _query.Value;
        await _toasts.EmitAsync(
            string.IsNullOrWhiteSpace(current)
                ? "Refreshed full coroutine catalog."
                : $"Refreshed results for '{current}'.",
            ct);
    }

    public async Task<string> FetchTrendingTipAsync(string query, CancellationToken ct = default)
    {
        await Coroutine.Delay(200, ct);
        return string.IsNullOrWhiteSpace(query)
            ? "Tip: Type 'Flow' or 'Scope' to see FlatMapLatest cancel in-flight searches."
            : $"Tip: Combine .Debounce() + .FlatMapLatest() when querying '{query}'.";
    }

    private async IAsyncEnumerable<SearchUiState> SearchCatalogAsync(
        string term,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(term))
        {
            yield return new SearchUiState(string.Empty, IsLoading: false, Catalog);
            yield break;
        }

        yield return new SearchUiState(term, IsLoading: true, []);

        // Simulated backend latency; automatically canceled if user types another character
        await Coroutine.Delay(350, ct);

        var matches = Catalog
            .Where(item => item.Contains(term, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        await _toasts.EmitAsync($"Found {matches.Length} match(es) for '{term}'.", ct);
        yield return new SearchUiState(term, IsLoading: false, matches);
    }

    public async ValueTask DisposeAsync()
    {
        await _viewModelScope.CancelAsync();
        await _viewModelScope.DisposeAsync();
    }
}

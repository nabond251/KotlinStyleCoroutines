using System.Runtime.CompilerServices;
using KotlinStyleCoroutines;

namespace BlazorSearchDemo;

public sealed record SearchUiState(
    string Query,
    bool IsLoading,
    IReadOnlyList<string> Results,
    string? ErrorMessage = null)
{
    public static SearchUiState Idle { get; } = new(string.Empty, false, []);
}

public sealed class SearchViewModel : IAsyncDisposable
{
    private readonly CoroutineScope _viewModelScope = Coroutine.CreateScope();
    private readonly MutableStateFlow<string> _query = new(string.Empty);
    private readonly MutableSharedFlow<string> _toasts = new(replay: 0, extraBufferCapacity: 16);

    public IStateFlow<string> Query => _query;

    public IStateFlow<SearchUiState> UiState { get; }

    public ISharedFlow<string> Toasts => _toasts;

    public SearchViewModel()
    {
        UiState = _query
            .Debounce(TimeSpan.FromMilliseconds(250))
            .FlatMapLatest(q => SearchCatalogAsync(q))
            .Catch(ex => new SearchUiState(_query.Value, false, [], ex.Message))
            .StateIn(
                _viewModelScope,
                SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.FromSeconds(5)),
                SearchUiState.Idle);
    }

    public void OnQueryChanged(string value)
    {
        _query.Value = value;
    }

    public async Task<string> FetchTrendingTipAsync(CancellationToken ct = default)
    {
        await Task.Delay(150, ct);
        return "Tip: Try typing 'kotlin', 'flow', or 'blazor' rapidly to see FlatMapLatest cancel stale searches.";
    }

    public async Task RefreshAsync(CancellationToken ct = default)
    {
        var current = _query.Value;
        await _toasts.EmitAsync(
            string.IsNullOrWhiteSpace(current)
                ? "Refreshed idle catalog."
                : $"Refreshed results for '{current}'.",
            ct);

        if (!string.IsNullOrWhiteSpace(current))
        {
            _query.Value = string.Empty;
            _query.Value = current;
        }
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

        yield return new SearchUiState(term, IsLoading: true, []);

        // Simulate backend search latency (canceled automatically by FlatMapLatest if user keeps typing)
        await Task.Delay(300, ct);

        if (term.Equals("error", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Simulated backend failure for query 'error'.");
        }

        string[] catalog =
        [
            "KotlinStyleCoroutines Core",
            "CoroutineScope & Structured Concurrency",
            "StateFlow & SharedFlow Hot Streams",
            "FlatMapLatest & Debounce Operators",
            "Blazor LifecycleComponentBase"
        ];

        var matches = catalog
            .Where(item => item.Contains(term, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        yield return new SearchUiState(term, IsLoading: false, matches);
    }

    public async ValueTask DisposeAsync()
    {
        await _viewModelScope.CancelAsync();
        await _viewModelScope.DisposeAsync();
    }
}

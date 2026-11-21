using System.Runtime.CompilerServices;
using KotlinStyleCoroutines;

namespace BlazorSearchDemo;

public sealed record SearchUiState(
    string Query,
    IReadOnlyList<string> Results,
    bool IsLoading,
    string? ErrorMessage);

public sealed class SearchViewModel : IAsyncDisposable
{
    private static readonly string[] Catalog =
    [
        "Kotlin Coroutines Guide",
        "Kotlin Flow & StateFlow",
        "Structured Concurrency in .NET 11",
        "Blazor Interactive Server Components",
        "Reactive UI Architecture",
        "C# 15 Async Enumerables",
        "ChannelFlow Fan-In Patterns"
    ];

    private readonly CancellationTokenSource _vmCts = new();
    private readonly CoroutineScope _viewModelScope;
    private readonly MutableStateFlow<string> _query = new(string.Empty);
    private readonly MutableSharedFlow<string> _toasts = new(replay: 0, extraBufferCapacity: 8);

    public SearchViewModel()
    {
        _viewModelScope = Coroutine.CreateScope(_vmCts.Token);

        var initialState = new SearchUiState(string.Empty, Catalog, IsLoading: false, ErrorMessage: null);

        // Cold pipeline converted to a hot StateFlow that pauses upstream work when no UI tab is subscribed
        UiState = _query
            .Debounce(TimeSpan.FromMilliseconds(250))
            .FlatMapLatest(SearchCatalogAsync)
            .Catch(ex => new SearchUiState(_query.Value, [], IsLoading: false, ErrorMessage: ex.Message))
            .StateIn(_viewModelScope, SharingStarted.WhileSubscribed(TimeSpan.FromSeconds(5)), initialState);
    }

    public IStateFlow<SearchUiState> UiState { get; }

    public ISharedFlow<string> Toasts => _toasts;

    public void OnQueryChanged(string newQuery) => _query.Value = newQuery;

    public async Task RefreshAsync(CancellationToken ct = default)
    {
        await _toasts.EmitAsync($"Refreshed catalog at {DateTime.Now:T}", ct);
    }

    public async Task<string> FetchTrendingTipAsync(string topic, CancellationToken ct)
    {
        await Task.Delay(300, ct);
        return string.IsNullOrWhiteSpace(topic)
            ? "Tip: Try typing 'Kotlin' or 'Blazor' to see Debounce + FlatMapLatest in action."
            : $"Showing live matches for "{topic}"";
    }

    private static async IAsyncEnumerable<SearchUiState> SearchCatalogAsync(
        string term,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var trimmed = term.Trim();
        if (string.IsNullOrEmpty(trimmed))
        {
            yield return new SearchUiState(term, Catalog, IsLoading: false, ErrorMessage: null);
            yield break;
        }

        // Emit immediate loading state before starting simulated network/DB search
        yield return new SearchUiState(term, [], IsLoading: true, ErrorMessage: null);

        await Task.Delay(350, ct);

        if (trimmed.Equals("error", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Simulated backend failure (caught by .Catch operator).");

        var matches = Catalog
            .Where(item => item.Contains(trimmed, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        yield return new SearchUiState(term, matches, IsLoading: false, ErrorMessage: null);
    }

    public async ValueTask DisposeAsync()
    {
        await _vmCts.CancelAsync();
        await _viewModelScope.DisposeAsync();
        _vmCts.Dispose();
    }
}

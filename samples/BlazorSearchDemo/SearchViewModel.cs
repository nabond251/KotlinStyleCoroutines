using System.Runtime.CompilerServices;
using KotlinStyleCoroutines;

namespace BlazorSearchDemo;

public sealed record SearchUiState(
    string Query,
    bool IsLoading,
    IReadOnlyList<string> Items,
    string? ErrorMessage = null)
{
    public static SearchUiState Idle { get; } = new(string.Empty, false, []);
}

public sealed class SearchViewModel : IAsyncDisposable
{
    private static readonly string[] Catalog =
    [
        "CoroutineScope",
        "SupervisorScope",
        "SuspendCancellable",
        "MutableStateFlow",
        "MutableSharedFlow",
        "SharingStarted.WhileSubscribed",
        "ChannelFlow",
        "FlatMapLatest",
        "TransformLatest",
        "CombineWith",
        "Debounce",
        "Sample",
        "RetryWhen",
        "CollectAsStateWithLifecycle",
        "LaunchedEffect",
        "ProduceState"
    ];

    private readonly CancellationTokenSource _vmCts = new();
    private readonly CoroutineScope _viewModelScope;
    private readonly MutableStateFlow<string> _query = new(string.Empty);
    private readonly MutableSharedFlow<string> _toasts = new(replay: 0, extraBufferCapacity: 8);

    public IStateFlow<string> Query => _query;
    public IStateFlow<SearchUiState> UiState { get; }
    public ISharedFlow<string> Toasts => _toasts;

    public SearchViewModel()
    {
        _viewModelScope = Coroutine.CreateScope(_vmCts.Token);

        UiState = _query
            .Debounce(TimeSpan.FromMilliseconds(250))
            .FlatMapLatest<string, SearchUiState>(SearchCatalogAsync)
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

    public async Task TriggerToastAsync(string message)
    {
        await _toasts.EmitAsync(message, _vmCts.Token);
    }

    public async Task<string> LoadTrendingSummaryAsync(string category, CancellationToken ct)
    {
        await Coroutine.Delay(300, ct);
        return $"Trending in {category}: FlatMapLatest & CollectAsStateWithLifecycle";
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

        yield return new SearchUiState(term, IsLoading: true, Items: []);

        // Simulate network latency; automatically canceled by FlatMapLatest if the user keeps typing
        await Coroutine.Delay(400, ct);

        if (term.Equals("error", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Simulated backend search failure.");
        }

        var matches = Catalog
            .Where(item => item.Contains(term, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        yield return new SearchUiState(term, IsLoading: false, Items: matches);
    }

    public async ValueTask DisposeAsync()
    {
        await _vmCts.CancelAsync();
        await _viewModelScope.DisposeAsync();
        _vmCts.Dispose();
    }
}

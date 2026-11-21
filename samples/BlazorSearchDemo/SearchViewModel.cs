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
        "Kotlin Coroutines Guide",
        "Structured Concurrency in .NET",
        "StateFlow & SharedFlow Patterns",
        "Jetpack Compose Lifecycle",
        "Blazor Interactive Server",
        "C# AsyncEnumerable Streams",
        "Channels & Backpressure"
    ];

    private readonly CoroutineScope _scope = Coroutine.CreateScope();
    private readonly MutableStateFlow<string> _query = new(string.Empty);
    private readonly MutableSharedFlow<string> _toasts = new(replay: 0, extraBufferCapacity: 8);

    public IStateFlow<string> Query => _query;
    public ISharedFlow<string> Toasts => _toasts;
    public IStateFlow<SearchUiState> State { get; }

    public SearchViewModel()
    {
        var initialState = new SearchUiState(string.Empty, IsLoading: false, Results: Catalog);

        State = _query
            .Debounce(TimeSpan.FromMilliseconds(250))
            .FlatMapLatest(q => SearchCatalogAsync(q))
            .Catch(ex => new SearchUiState(_query.Value, IsLoading: false, Results: [], ErrorMessage: ex.Message))
            .StateIn(_scope, SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.FromSeconds(5)), initialState);
    }

    public void OnQueryChanged(string value)
    {
        _query.Value = value;
    }

    public async Task NotifySavedAsync(string item)
    {
        await _toasts.EmitAsync($"Bookmarked \"{item}\"", _scope.Token);
    }

    private static async IAsyncEnumerable<SearchUiState> SearchCatalogAsync(
        string term,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(term))
        {
            yield return new SearchUiState(string.Empty, IsLoading: false, Results: Catalog);
            yield break;
        }

        yield return new SearchUiState(term, IsLoading: true, Results: []);

        // Simulated backend latency; automatically canceled by FlatMapLatest if a newer query arrives
        await Coroutine.Delay(300, ct);

        if (term.Equals("error", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Simulated backend failure for query 'error'.");
        }

        var matches = Catalog
            .Where(item => item.Contains(term, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        yield return new SearchUiState(term, IsLoading: false, Results: matches);
    }

    public async ValueTask DisposeAsync()
    {
        await _scope.CancelAsync();
        await _scope.DisposeAsync();
    }
}

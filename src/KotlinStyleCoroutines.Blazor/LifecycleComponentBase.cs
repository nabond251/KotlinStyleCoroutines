using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace KotlinStyleCoroutines.Blazor;

/// <summary>
/// Base Blazor component providing Jetpack Compose-style coroutine and Flow bridges:
/// <see cref="ComponentScope"/>, <see cref="CollectAsStateWithLifecycle{T}(IStateFlow{T}, bool)"/>,
/// <see cref="LaunchedEffect{TKey}"/>, and <see cref="ProduceState{T, TKey}"/>.
/// </summary>
public abstract class LifecycleComponentBase : ComponentBase, IAsyncDisposable
{
    private readonly CancellationTokenSource _lifecycleCts = new();
    private readonly List<Action<bool>> _visibilityListeners = [];
    private readonly Dictionary<string, (object? Key, Job Job)> _effects = [];
    private readonly Dictionary<string, object> _producedStates = [];

    private IJSObjectReference? _visibilityModule;
    private IJSObjectReference? _visibilitySubscription;
    private DotNetObjectReference<LifecycleComponentBase>? _dotNetRef;
    private bool _isVisible = true;
    private bool _disposed;

    [Inject]
    private IJSRuntime JS { get; set; } = default!;

    /// <summary>
    /// Structured concurrency scope bound to this component's lifetime.
    /// Automatically canceled and drained when the component is disposed.
    /// </summary>
    protected CoroutineScope ComponentScope { get; private set; } = default!;

    protected override void OnInitialized()
    {
        ComponentScope = Coroutine.CreateScope(_lifecycleCts.Token);
        base.OnInitialized();
    }

    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (firstRender && _visibilityListeners.Count > 0)
        {
            _dotNetRef = DotNetObjectReference.Create(this);
            _visibilityModule = await JS.InvokeAsync<IJSObjectReference>(
                "import",
                "./_content/KotlinStyleCoroutines.Blazor/visibility.js");
            _visibilitySubscription = await _visibilityModule.InvokeAsync<IJSObjectReference>(
                "observeVisibility",
                _dotNetRef);
        }

        await base.OnAfterRenderAsync(firstRender);
    }

    /// <summary>
    /// Invoked from JavaScript when the browser tab transitions between visible (<c>STARTED</c>)
    /// and hidden (<c>STOPPED</c>).
    /// </summary>
    [JSInvokable]
    public void OnVisibilityChanged(bool isVisible)
    {
        if (_disposed || _isVisible == isVisible)
            return;

        _isVisible = isVisible;

        foreach (var listener in _visibilityListeners)
            listener(isVisible);
    }

    /// <summary>
    /// Collects an <see cref="IStateFlow{T}"/> into a reactive <see cref="FlowState{T}"/>,
    /// triggering <see cref="ComponentBase.StateHasChanged"/> on updates and pausing collection
    /// when the browser tab is hidden.
    /// </summary>
    protected FlowState<T> CollectAsStateWithLifecycle<T>(
        IStateFlow<T> stateFlow,
        bool pauseWhenTabHidden = true)
    {
        ArgumentNullException.ThrowIfNull(stateFlow);
        return CollectAsStateWithLifecycle(stateFlow, stateFlow.Value, pauseWhenTabHidden);
    }

    /// <summary>
    /// Collects any cold or hot <see cref="IAsyncEnumerable{T}"/> into a reactive <see cref="FlowState{T}"/>,
    /// triggering <see cref="ComponentBase.StateHasChanged"/> on updates and pausing collection
    /// when the browser tab is hidden.
    /// </summary>
    protected FlowState<T> CollectAsStateWithLifecycle<T>(
        IAsyncEnumerable<T> flow,
        T initialValue,
        bool pauseWhenTabHidden = true)
    {
        ArgumentNullException.ThrowIfNull(flow);

        var state = new FlowState<T>(initialValue);
        Job? activeJob = null;

        void StartCollection()
        {
            if (_disposed || activeJob is { IsActive: true })
                return;

            activeJob = flow.LaunchIn(ComponentScope, async (item, _) =>
            {
                state.Value = item;
                await InvokeAsync(StateHasChanged);
            });
        }

        void StopCollection()
        {
            activeJob?.Cancel();
            activeJob = null;
        }

        StartCollection();

        if (pauseWhenTabHidden)
        {
            _visibilityListeners.Add(visible =>
            {
                if (visible)
                    StartCollection();
                else
                    StopCollection();
            });
        }

        return state;
    }

    /// <summary>
    /// Analogous to Jetpack Compose's <c>LaunchedEffect(key)</c>.
    /// Launches a child coroutine scoped to this component and cancels/restarts it whenever
    /// <paramref name="key"/> changes.
    /// </summary>
    protected void LaunchedEffect<TKey>(
        string id,
        TKey key,
        Func<CancellationToken, Task> block)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        ArgumentNullException.ThrowIfNull(block);

        if (_effects.TryGetValue(id, out var existing))
        {
            if (EqualityComparer<TKey>.Default.Equals((TKey?)existing.Key, key))
                return;

            existing.Job.Cancel();
        }

        _effects[id] = (key, ComponentScope.Launch(block));
    }

    /// <summary>
    /// Analogous to Jetpack Compose's <c>produceState(initialValue, key)</c>.
    /// Runs an asynchronous producer whenever <paramref name="key"/> changes and updates the
    /// returned <see cref="FlowState{T}"/> upon completion.
    /// </summary>
    protected FlowState<T> ProduceState<T, TKey>(
        string id,
        T initialValue,
        TKey key,
        Func<CancellationToken, Task<T>> producer)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        ArgumentNullException.ThrowIfNull(producer);

        if (!_producedStates.TryGetValue(id, out var existingState) || existingState is not FlowState<T> state)
        {
            state = new FlowState<T>(initialValue);
            _producedStates[id] = state;
        }

        LaunchedEffect($"__produce:{id}", key, async ct =>
        {
            var result = await producer(ct);
            state.Value = result;
            await InvokeAsync(StateHasChanged);
        });

        return state;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        await _lifecycleCts.CancelAsync();
        await ComponentScope.DisposeAsync();
        _lifecycleCts.Dispose();

        try
        {
            if (_visibilitySubscription is not null)
                await _visibilitySubscription.DisposeAsync();

            if (_visibilityModule is not null)
                await _visibilityModule.DisposeAsync();
        }
        catch (Exception ex) when (ex is JSDisconnectedException or OperationCanceledException)
        {
            // Browser circuit disconnected during teardown
        }

        _dotNetRef?.Dispose();
        GC.SuppressFinalize(this);
    }
}

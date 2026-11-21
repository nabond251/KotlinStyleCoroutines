using KotlinStyleCoroutines;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace KotlinStyleCoroutines.Blazor;

public abstract class LifecycleComponentBase : ComponentBase, IAsyncDisposable
{
    private readonly CancellationTokenSource _lifecycleCts = new();
    private readonly List<Action<bool>> _visibilityListeners = [];
    private readonly Dictionary<string, (object? Key, Job Job)> _effects = [];
    private IJSObjectReference? _visibilityModule;
    private IJSObjectReference? _visibilityHandle;
    private DotNetObjectReference<LifecycleComponentBase>? _dotNetRef;
    private bool _isVisible = true;

    [Inject] private IJSRuntime JS { get; set; } = default!;

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
            try
            {
                _dotNetRef = DotNetObjectReference.Create(this);
                _visibilityModule = await JS.InvokeAsync<IJSObjectReference>(
                    "import", "./_content/KotlinStyleCoroutines.Blazor/visibility.js");
                _visibilityHandle = await _visibilityModule.InvokeAsync<IJSObjectReference>(
                    "registerVisibility", _dotNetRef);
            }
            catch (Exception ex) when (ex is JSException or JSDisconnectedException or TaskCanceledException or InvalidOperationException)
            {
                // Ignore JS interop failures during static prerendering or circuit disconnection
            }
        }

        await base.OnAfterRenderAsync(firstRender);
    }

    [JSInvokable]
    public void OnVisibilityChanged(bool isVisible)
    {
        if (_isVisible == isVisible)
            return;

        _isVisible = isVisible;
        foreach (var listener in _visibilityListeners)
        {
            listener(isVisible);
        }
    }

    /// <summary>
    /// Collects an <see cref="IStateFlow{T}"/> into a reactive <see cref="FlowState{T}"/>,
    /// triggering <see cref="ComponentBase.StateHasChanged"/> on emissions and pausing collection
    /// when the browser tab becomes hidden.
    /// </summary>
    protected FlowState<T> CollectAsStateWithLifecycle<T>(
        IStateFlow<T> stateFlow,
        bool pauseWhenTabHidden = true)
        => CollectAsStateWithLifecycle(stateFlow, stateFlow.Value, pauseWhenTabHidden);

    /// <summary>
    /// Collects any cold or hot <see cref="IAsyncEnumerable{T}"/> into a reactive <see cref="FlowState{T}"/>,
    /// triggering <see cref="ComponentBase.StateHasChanged"/> on emissions and pausing collection
    /// when the browser tab becomes hidden.
    /// </summary>
    protected FlowState<T> CollectAsStateWithLifecycle<T>(
        IAsyncEnumerable<T> flow,
        T initialValue,
        bool pauseWhenTabHidden = true)
    {
        var state = new FlowState<T>(initialValue);
        Job? activeJob = null;

        void StartCollection()
        {
            if (activeJob is { IsActive: true })
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
    /// Launches a coroutine tied to <see cref="ComponentScope"/> and automatically cancels and restarts it
    /// whenever <paramref name="key"/> changes across parameter updates.
    /// </summary>
    protected void LaunchedEffect<TKey>(string id, TKey key, Func<CancellationToken, Task> block)
    {
        if (_effects.TryGetValue(id, out var existing))
        {
            if (EqualityComparer<TKey>.Default.Equals((TKey?)existing.Key, key))
                return;

            existing.Job.Cancel();
        }

        _effects[id] = (key, ComponentScope.Launch(block));
    }

    /// <summary>
    /// Executes an asynchronous producer whenever <paramref name="key"/> changes and exposes the result
    /// as a reactive <see cref="FlowState{T}"/>.
    /// </summary>
    protected FlowState<T> ProduceState<T, TKey>(
        T initialValue,
        string id,
        TKey key,
        Func<CancellationToken, Task<T>> producer)
    {
        var state = new FlowState<T>(initialValue);

        LaunchedEffect(id, key, async ct =>
        {
            var value = await producer(ct);
            state.Value = value;
            await InvokeAsync(StateHasChanged);
        });

        return state;
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycleCts.CancelAsync();

        if (ComponentScope is not null)
        {
            await ComponentScope.DisposeAsync();
        }

        _lifecycleCts.Dispose();

        try
        {
            if (_visibilityHandle is not null)
            {
                await _visibilityHandle.InvokeVoidAsync("dispose");
                await _visibilityHandle.DisposeAsync();
            }

            if (_visibilityModule is not null)
            {
                await _visibilityModule.DisposeAsync();
            }
        }
        catch (Exception ex) when (ex is JSException or JSDisconnectedException or TaskCanceledException or InvalidOperationException)
        {
            // Circuit may already be disconnected during disposal
        }

        _dotNetRef?.Dispose();
        GC.SuppressFinalize(this);
    }
}

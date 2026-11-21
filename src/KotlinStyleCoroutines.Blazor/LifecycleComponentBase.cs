using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace KotlinStyleCoroutines.Blazor;

/// <summary>
/// Base Blazor component providing structured concurrency (<see cref="ComponentScope"/>) and
/// lifecycle-aware coroutine/flow collection helpers modeled after Jetpack Compose.
/// </summary>
public abstract class LifecycleComponentBase : ComponentBase, IAsyncDisposable
{
    private readonly CancellationTokenSource _lifecycleCts = new();
    private readonly List<Action<bool>> _visibilityListeners = [];
    private readonly Dictionary<string, (object? Key, Job Job)> _effects = [];
    private IJSObjectReference? _visibilityModule;
    private IJSObjectReference? _visibilitySubscription;
    private DotNetObjectReference<LifecycleComponentBase>? _dotNetRef;
    private bool _isVisible = true;

    [Inject] private IJSRuntime JS { get; set; } = default!;

    /// <summary>
    /// Gets the structured <see cref="CoroutineScope"/> tied to this component's mount/unmount lifecycle.
    /// All child coroutines launched in this scope are automatically canceled and awaited in <see cref="DisposeAsync"/>.
    /// </summary>
    protected CoroutineScope ComponentScope { get; private set; } = default!;

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        ComponentScope = Coroutine.CreateScope(_lifecycleCts.Token);
        base.OnInitialized();
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (firstRender && _visibilityListeners.Count > 0)
        {
            try
            {
                _dotNetRef = DotNetObjectReference.Create(this);
                _visibilityModule = await JS.InvokeAsync<IJSObjectReference>(
                    "import",
                    "./_content/KotlinStyleCoroutines.Blazor/visibility.js");
                _visibilitySubscription = await _visibilityModule.InvokeAsync<IJSObjectReference>(
                    "registerVisibilityListener",
                    _dotNetRef);
            }
            catch (Exception ex) when (ex is JSException or JSDisconnectedException or TaskCanceledException or InvalidOperationException)
            {
                // Ignore JS interop failures during static prerendering or circuit disconnection.
            }
        }

        await base.OnAfterRenderAsync(firstRender);
    }

    /// <summary>
    /// Invoked from JavaScript when the browser tab's <c>document.visibilitychange</c> state changes.
    /// </summary>
    /// <param name="isVisible"><see langword="true"/> when the page is visible; <see langword="false"/> when hidden.</param>
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
    /// Collects values from an <see cref="IStateFlow{T}"/> and represents its latest value as a <see cref="FlowState{T}"/>,
    /// automatically triggering <see cref="ComponentBase.StateHasChanged"/> on updates and pausing when the tab is hidden.
    /// </summary>
    /// <typeparam name="T">The type of state emitted by <paramref name="stateFlow"/>.</typeparam>
    /// <param name="stateFlow">The hot state flow to observe.</param>
    /// <param name="pauseWhenTabHidden">
    /// When <see langword="true"/>, cancels active collection while the browser tab is hidden and restarts when visible.
    /// </param>
    /// <returns>A <see cref="FlowState{T}"/> holding the latest value.</returns>
    protected FlowState<T> CollectAsStateWithLifecycle<T>(
        IStateFlow<T> stateFlow,
        bool pauseWhenTabHidden = true)
    {
        ArgumentNullException.ThrowIfNull(stateFlow);
        return CollectAsStateWithLifecycle(stateFlow, stateFlow.Value, pauseWhenTabHidden);
    }

    /// <summary>
    /// Collects values from an <see cref="IAsyncEnumerable{T}"/> flow and represents its latest value as a <see cref="FlowState{T}"/>,
    /// automatically triggering <see cref="ComponentBase.StateHasChanged"/> on updates and pausing when the tab is hidden.
    /// </summary>
    /// <typeparam name="T">The type of element emitted by <paramref name="flow"/>.</typeparam>
    /// <param name="flow">The cold or hot flow to collect.</param>
    /// <param name="initialValue">The initial value exposed before the first element is emitted.</param>
    /// <param name="pauseWhenTabHidden">
    /// When <see langword="true"/>, cancels active collection while the browser tab is hidden and restarts when visible.
    /// </param>
    /// <returns>A <see cref="FlowState{T}"/> holding the latest value.</returns>
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
    /// Launches a side-effect coroutine tied to <paramref name="id"/> and <paramref name="key"/>.
    /// If called again with a different <paramref name="key"/>, the previous coroutine is canceled and restarted.
    /// </summary>
    /// <typeparam name="TKey">The type of key used to track effect restarts.</typeparam>
    /// <param name="id">A unique identifier for this effect within the component.</param>
    /// <param name="key">The dependency key that triggers a restart when changed.</param>
    /// <param name="block">The asynchronous side-effect delegate to run.</param>
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
    /// Launches a coroutine that produces a <see cref="FlowState{T}"/> from an asynchronous computation
    /// and restarts whenever <paramref name="key"/> changes.
    /// </summary>
    /// <typeparam name="T">The type of value produced.</typeparam>
    /// <typeparam name="TKey">The type of key used to track recomputation.</typeparam>
    /// <param name="id">A unique identifier for this producer within the component.</param>
    /// <param name="initialValue">The initial value exposed while <paramref name="producer"/> is running.</param>
    /// <param name="key">The dependency key that triggers recomputation when changed.</param>
    /// <param name="producer">The asynchronous computation producing the state value.</param>
    /// <returns>A <see cref="FlowState{T}"/> updated when <paramref name="producer"/> completes.</returns>
    protected FlowState<T> ProduceState<T, TKey>(
        string id,
        T initialValue,
        TKey key,
        Func<CancellationToken, Task<T>> producer)
    {
        ArgumentNullException.ThrowIfNull(producer);

        var state = new FlowState<T>(initialValue);

        LaunchedEffect(id, key, async ct =>
        {
            var result = await producer(ct);
            state.Value = result;
            await InvokeAsync(StateHasChanged);
        });

        return state;
    }

    /// <inheritdoc />
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
            if (_visibilitySubscription is not null)
            {
                await _visibilitySubscription.InvokeVoidAsync("dispose");
                await _visibilitySubscription.DisposeAsync();
            }

            if (_visibilityModule is not null)
            {
                await _visibilityModule.DisposeAsync();
            }
        }
        catch (Exception ex) when (ex is JSException or JSDisconnectedException or TaskCanceledException or InvalidOperationException)
        {
            // Ignore JS interop failures when the circuit is already disconnected.
        }

        _dotNetRef?.Dispose();
        GC.SuppressFinalize(this);
    }
}

using KotlinStyleCoroutines;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace KotlinStyleCoroutines.Blazor;

/// <summary>
/// Base Blazor component providing a component-scoped <see cref="CoroutineScope"/>,
/// lifecycle-aware Flow collection (<see cref="CollectAsStateWithLifecycle{T}(IStateFlow{T}, bool)"/>),
/// keyed side effects (<see cref="LaunchedEffect{TKey}"/>), and async state producers (<see cref="ProduceState{T, TKey}"/>).
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
    private bool _disposed;

    [Inject]
    private IJSRuntime JS { get; set; } = default!;

    /// <summary>
    /// Gets the <see cref="CoroutineScope"/> bound to this component's lifetime.
    /// All coroutines launched on this scope are automatically canceled and awaited when the component is disposed.
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
    /// Invoked when the browser tab visibility changes (or directly in unit tests) to pause or resume active collectors.
    /// </summary>
    /// <param name="isVisible"><see langword="true"/> when the page becomes visible; <see langword="false"/> when hidden.</param>
    [JSInvokable]
    public void OnVisibilityChanged(bool isVisible)
    {
        if (_disposed || _isVisible == isVisible)
        {
            return;
        }

        _isVisible = isVisible;

        foreach (var listener in _visibilityListeners)
        {
            listener(isVisible);
        }
    }

    /// <summary>
    /// Subscribes to an <see cref="IStateFlow{T}"/> using its current <see cref="IStateFlow{T}.Value"/> as initial state,
    /// triggers <see cref="ComponentBase.StateHasChanged"/> on updates, and pauses collection while the tab is hidden.
    /// </summary>
    /// <typeparam name="T">The type of state emitted by the flow.</typeparam>
    /// <param name="stateFlow">The hot state flow to observe.</param>
    /// <param name="pauseWhenTabHidden">Whether to cancel collection when the browser tab becomes hidden and restart when visible.</param>
    /// <returns>A reactive <see cref="FlowState{T}"/> wrapper for Razor rendering.</returns>
    protected FlowState<T> CollectAsStateWithLifecycle<T>(
        IStateFlow<T> stateFlow,
        bool pauseWhenTabHidden = true)
    {
        ArgumentNullException.ThrowIfNull(stateFlow);
        return CollectAsStateWithLifecycle(stateFlow, stateFlow.Value, pauseWhenTabHidden);
    }

    /// <summary>
    /// Subscribes to any cold or hot <see cref="IAsyncEnumerable{T}"/> stream, triggers <see cref="ComponentBase.StateHasChanged"/>
    /// on each emitted element, and optionally pauses collection while the browser tab is hidden.
    /// </summary>
    /// <typeparam name="T">The type of elements emitted by the flow.</typeparam>
    /// <param name="flow">The asynchronous stream to collect.</param>
    /// <param name="initialValue">The initial value exposed before the first element arrives.</param>
    /// <param name="pauseWhenTabHidden">Whether to cancel collection when the browser tab becomes hidden and restart when visible.</param>
    /// <returns>A reactive <see cref="FlowState{T}"/> wrapper for Razor rendering.</returns>
    protected FlowState<T> CollectAsStateWithLifecycle<T>(
        IAsyncEnumerable<T> flow,
        T initialValue,
        bool pauseWhenTabHidden = true)
    {
        ArgumentNullException.ThrowIfNull(flow);

        var state = new FlowState<T>(initialValue);
        var gate = new Lock();
        Job? activeJob = null;

        void StartCollection()
        {
            lock (gate)
            {
                if (_disposed || activeJob is { IsActive: true })
                {
                    return;
                }

                activeJob = flow.LaunchIn(ComponentScope, async (item, _) =>
                {
                    state.Value = item;
                    await InvokeAsync(StateHasChanged);
                });
            }
        }

        void StopCollection()
        {
            Job? jobToCancel;
            lock (gate)
            {
                jobToCancel = activeJob;
                activeJob = null;
            }

            jobToCancel?.Cancel();
        }

        if (_isVisible)
        {
            StartCollection();
        }

        if (pauseWhenTabHidden)
        {
            _visibilityListeners.Add(visible =>
            {
                if (visible)
                {
                    StartCollection();
                }
                else
                {
                    StopCollection();
                }
            });
        }

        return state;
    }

    /// <summary>
    /// Launches a side-effect coroutine bound to <see cref="ComponentScope"/> and automatically cancels and restarts it
    /// whenever <paramref name="key"/> changes across renders.
    /// </summary>
    /// <typeparam name="TKey">The type of the key controlling effect restarts.</typeparam>
    /// <param name="id">A unique identifier for this effect within the component.</param>
    /// <param name="key">The current key value; changing this value cancels the previous run and starts a new one.</param>
    /// <param name="block">The cancellable asynchronous effect body.</param>
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
            {
                return;
            }

            existing.Job.Cancel();
        }

        _effects[id] = (key, ComponentScope.Launch(block));
    }

    /// <summary>
    /// Converts a keyed asynchronous producer into a reactive <see cref="FlowState{T}"/> that automatically
    /// cancels in-flight work and re-runs whenever <paramref name="key"/> changes.
    /// </summary>
    /// <typeparam name="T">The type of produced state.</typeparam>
    /// <typeparam name="TKey">The type of the key controlling producer restarts.</typeparam>
    /// <param name="id">A unique identifier for this state producer within the component.</param>
    /// <param name="initialValue">The initial value exposed while the producer is running.</param>
    /// <param name="key">The current key value; changing this value cancels the previous producer and starts a new one.</param>
    /// <param name="producer">The cancellable asynchronous producer function.</param>
    /// <returns>A reactive <see cref="FlowState{T}"/> updated when <paramref name="producer"/> completes.</returns>
    protected FlowState<T> ProduceState<T, TKey>(
        string id,
        T initialValue,
        TKey key,
        Func<CancellationToken, Task<T>> producer)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
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
        if (_disposed)
        {
            return;
        }

        _disposed = true;

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
            // Ignore JS interop failures during circuit shutdown.
        }

        _dotNetRef?.Dispose();
        GC.SuppressFinalize(this);
    }
}

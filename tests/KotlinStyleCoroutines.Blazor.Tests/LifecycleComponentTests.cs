using Bunit;
using KotlinStyleCoroutines.Blazor;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Xunit;

namespace KotlinStyleCoroutines.Blazor.Tests;

public sealed class LifecycleComponentTests : BunitContext
{
    public LifecycleComponentTests()
    {
        JSInterop.Mode = JSRuntimeMode.Loose;
    }

    [Fact]
    public async Task CollectAsStateWithLifecycleRendersInitialAndUpdatedStateFlowValues()
    {
        var state = new MutableStateFlow<string>("Idle");

        var cut = Render<StateFlowTestComponent>(parameters => parameters
            .Add(p => p.Source, state));

        Assert.Equal("Idle", cut.Find("p").TextContent);

        state.Value = "Loaded";
        await WaitForTextAsync(cut, "p", "Loaded");
    }

    [Fact]
    public async Task CollectAsStateWithLifecyclePausesAndResumesOnTabVisibilityChange()
    {
        var upstreamStarts = 0;
        var upstreamStops = 0;

        async IAsyncEnumerable<int> CounterFlow(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            var startNum = Interlocked.Increment(ref upstreamStarts);
            var count = 0;
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    count++;
                    yield return (startNum * 100) + count;
                    await Task.Delay(25, ct);
                }
            }
            finally
            {
                Interlocked.Increment(ref upstreamStops);
            }
        }

        await using var sharingScope = Coroutine.CreateScope();
        var shared = CounterFlow().ShareIn(
            sharingScope,
            SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.Zero),
            replay: 1);

        var cut = Render<SharedFlowTestComponent>(parameters => parameters
            .Add(p => p.Source, shared));

        await WaitForConditionAsync(() => Volatile.Read(ref upstreamStarts) == 1);
        Assert.Equal(1, shared.SubscriptionCount.Value);

        // Simulate browser tab becoming hidden (Lifecycle STOPPED)
        await cut.InvokeAsync(() => cut.Instance.OnVisibilityChanged(false));

        await WaitForConditionAsync(() => shared.SubscriptionCount.Value == 0);
        await WaitForConditionAsync(() => Volatile.Read(ref upstreamStops) == 1);

        // Simulate browser tab becoming visible again (Lifecycle STARTED)
        await cut.InvokeAsync(() => cut.Instance.OnVisibilityChanged(true));

        await WaitForConditionAsync(() => shared.SubscriptionCount.Value == 1);
        await WaitForConditionAsync(() => Volatile.Read(ref upstreamStarts) == 2);

        await DisposeComponentsAsync();
        await sharingScope.CancelAsync();
        await sharingScope.DisposeAsync();
    }

    [Fact]
    public async Task DisposeComponentsAsyncCancelsComponentScopeJobs()
    {
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var cut = Render<ScopedJobTestComponent>(parameters => parameters
            .Add(p => p.OnCancelled, () => cancelled.TrySetResult()));

        Assert.True(cut.Instance.LaunchedJob.IsActive);

        await DisposeComponentsAsync();

        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(cut.Instance.LaunchedJob.IsCompleted);
    }

    [Fact]
    public async Task LaunchedEffectCancelsPreviousJobAndRestartsWhenKeyChanges()
    {
        var cancelledKeys = new List<string>();
        var startedKeys = new List<string>();

        var cut = Render<LaunchedEffectTestComponent>(parameters => parameters
            .Add(p => p.ItemId, "alpha")
            .Add(p => p.OnStarted, key =>
            {
                lock (startedKeys)
                {
                    startedKeys.Add(key);
                }
            })
            .Add(p => p.OnCancelled, key =>
            {
                lock (cancelledKeys)
                {
                    cancelledKeys.Add(key);
                }
            }));

        await WaitForConditionAsync(() =>
        {
            lock (startedKeys)
            {
                return startedKeys.Contains("alpha");
            }
        });

        // Re-render with the same key: should NOT restart
        cut.Render(parameters => parameters
            .Add(p => p.ItemId, "alpha"));

        await Task.Delay(30);
        lock (startedKeys)
        {
            Assert.Single(startedKeys);
        }

        // Re-render with a new key: should cancel "alpha" and start "beta"
        cut.Render(parameters => parameters
            .Add(p => p.ItemId, "beta"));

        await WaitForConditionAsync(() =>
        {
            lock (cancelledKeys)
            lock (startedKeys)
            {
                return cancelledKeys.Contains("alpha") && startedKeys.Contains("beta");
            }
        });
    }

    [Fact]
    public async Task ProduceStateResolvesAsyncValueAndUpdatesWhenKeyChanges()
    {
        var cut = Render<ProduceStateTestComponent>(parameters => parameters
            .Add(p => p.Category, "Coroutines"));

        Assert.Equal("Loading...", cut.Find("span").TextContent);

        await WaitForTextAsync(cut, "span", "Tip for Coroutines");

        cut.Render(parameters => parameters
            .Add(p => p.Category, "Flows"));

        await WaitForTextAsync(cut, "span", "Tip for Flows");
    }

    private static async Task WaitForTextAsync<TComponent>(
        IRenderedComponent<TComponent> cut,
        string selector,
        string expectedText)
        where TComponent : IComponent
    {
        await WaitForConditionAsync(() => cut.Find(selector).TextContent == expectedText);
    }

    private static async Task WaitForConditionAsync(Func<bool> predicate)
    {
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (!predicate())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for bUnit component state condition.");
            }

            await Task.Delay(15);
        }
    }

    private sealed class StateFlowTestComponent : LifecycleComponentBase
    {
        private FlowState<string> _state = default!;

        [Parameter, EditorRequired]
        public IStateFlow<string> Source { get; set; } = default!;

        protected override void OnInitialized()
        {
            base.OnInitialized();
            _state = CollectAsStateWithLifecycle(Source, pauseWhenTabHidden: false);
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, _state.Value);
            builder.CloseElement();
        }
    }

    private sealed class SharedFlowTestComponent : LifecycleComponentBase
    {
        private FlowState<int> _state = default!;

        [Parameter, EditorRequired]
        public ISharedFlow<int> Source { get; set; } = default!;

        protected override void OnInitialized()
        {
            base.OnInitialized();
            _state = CollectAsStateWithLifecycle(Source, initialValue: 0, pauseWhenTabHidden: true);
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, _state.Value);
            builder.CloseElement();
        }
    }

    private sealed class ScopedJobTestComponent : LifecycleComponentBase
    {
        [Parameter, EditorRequired]
        public Action OnCancelled { get; set; } = default!;

        public Job LaunchedJob { get; private set; } = default!;

        protected override void OnInitialized()
        {
            base.OnInitialized();
            LaunchedJob = ComponentScope.Launch(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                catch (OperationCanceledException)
                {
                    OnCancelled();
                    throw;
                }
            });
        }
    }

    private sealed class LaunchedEffectTestComponent : LifecycleComponentBase
    {
        [Parameter, EditorRequired]
        public string ItemId { get; set; } = default!;

        [Parameter, EditorRequired]
        public Action<string> OnStarted { get; set; } = default!;

        [Parameter, EditorRequired]
        public Action<string> OnCancelled { get; set; } = default!;

        protected override void OnParametersSet()
        {
            base.OnParametersSet();
            var capturedKey = ItemId;
            LaunchedEffect("item-loader", capturedKey, async ct =>
            {
                OnStarted(capturedKey);
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                catch (OperationCanceledException)
                {
                    OnCancelled(capturedKey);
                    throw;
                }
            });
        }
    }

    private sealed class ProduceStateTestComponent : LifecycleComponentBase
    {
        private FlowState<string> _tip = default!;

        [Parameter, EditorRequired]
        public string Category { get; set; } = default!;

        protected override void OnParametersSet()
        {
            base.OnParametersSet();
            var capturedCategory = Category;
            _tip = ProduceState("produce-tip", capturedCategory, "Loading...", async ct =>
            {
                await Task.Delay(20, ct);
                return $"Tip for {capturedCategory}";
            });
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "span");
            builder.AddContent(1, _tip.Value);
            builder.CloseElement();
        }
    }
}

using System.Runtime.CompilerServices;
using Bunit;
using KotlinStyleCoroutines;
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
    public void CollectAsStateWithLifecycleRendersInitialAndUpdatedStateFlowValues()
    {
        var stateFlow = new MutableStateFlow<string>("Initial");
        var cut = Render<StateFlowTestComponent>(p => p.Add(c => c.StateFlow, stateFlow));

        cut.WaitForAssertion(() => Assert.Equal("Initial", cut.Find("p").TextContent));

        stateFlow.Value = "Updated";

        cut.WaitForAssertion(() => Assert.Equal("Updated", cut.Find("p").TextContent));
    }

    [Fact]
    public async Task CollectAsStateWithLifecyclePausesAndResumesOnTabVisibility()
    {
        var upstreamStarts = 0;

        static async IAsyncEnumerable<int> ColdCounter([EnumeratorCancellation] CancellationToken ct = default)
        {
            var i = 1;
            while (!ct.IsCancellationRequested)
            {
                yield return i++;
                await Task.Delay(15, ct);
            }
        }

        async IAsyncEnumerable<int> TrackedColdCounter([EnumeratorCancellation] CancellationToken ct = default)
        {
            Interlocked.Increment(ref upstreamStarts);
            await foreach (var item in ColdCounter(ct))
            {
                yield return item;
            }
        }

        using var scopeCts = new CancellationTokenSource();

        await Coroutine.Scope(async scope =>
        {
            var shared = TrackedColdCounter().ShareIn(
                scope,
                SharingStarted.WhileSubscribed(),
                replay: 1);

            var cut = Render<AsyncEnumerableTestComponent>(p => p
                .Add(c => c.Flow, shared)
                .Add(c => c.InitialValue, 0));

            await WaitUntilAsync(() => shared.SubscriptionCount.Value == 1 && Volatile.Read(ref upstreamStarts) == 1);
            cut.WaitForAssertion(() => Assert.NotEqual("0", cut.Find("p").TextContent));

            // Simulate browser tab becoming hidden (Lifecycle.State.STOPPED)
            cut.Instance.OnVisibilityChanged(false);

            await WaitUntilAsync(() => shared.SubscriptionCount.Value == 0);
            var pausedValue = cut.Find("p").TextContent;
            await Task.Delay(50);
            Assert.Equal(pausedValue, cut.Find("p").TextContent);

            // Simulate browser tab becoming visible again (Lifecycle.State.STARTED)
            cut.Instance.OnVisibilityChanged(true);

            await WaitUntilAsync(() => shared.SubscriptionCount.Value == 1 && Volatile.Read(ref upstreamStarts) == 2);

            await DisposeComponentsAsync();
            await scopeCts.CancelAsync();
        }, scopeCts.Token);
    }

    [Fact]
    public async Task DisposeComponentsAsyncCancelsComponentScopeJobs()
    {
        var cut = Render<BackgroundJobTestComponent>();
        var backgroundJob = cut.Instance.BackgroundJob;

        Assert.NotNull(backgroundJob);
        Assert.True(backgroundJob.IsActive);

        await DisposeComponentsAsync();

        Assert.True(backgroundJob.IsCanceled);
        Assert.True(backgroundJob.IsCompleted);
    }

    [Fact]
    public void LaunchedEffectCancelsPreviousAndRestartsWhenKeyChanges()
    {
        var canceledKeys = new List<string>();
        var startedKeys = new List<string>();

        var cut = Render<LaunchedEffectTestComponent>(p => p
            .Add(c => c.QueryKey, "alpha")
            .Add(c => c.OnStarted, k => startedKeys.Add(k))
            .Add(c => c.OnCanceled, k => canceledKeys.Add(k)));

        cut.WaitForAssertion(() => Assert.Contains("alpha", startedKeys));

        cut.Render(p => p
            .Add(c => c.QueryKey, "beta")
            .Add(c => c.OnStarted, k => startedKeys.Add(k))
            .Add(c => c.OnCanceled, k => canceledKeys.Add(k)));

        cut.WaitForAssertion(() =>
        {
            Assert.Contains("alpha", canceledKeys);
            Assert.Contains("beta", startedKeys);
        });
    }

    [Fact]
    public void ProduceStateResolvesAsyncValueAndUpdatesWhenKeyChanges()
    {
        var cut = Render<ProduceStateTestComponent>(p => p.Add(c => c.ItemKey, "one"));

        Assert.Equal("Loading...", cut.Find("p").TextContent);

        cut.WaitForAssertion(() => Assert.Equal("Loaded:one", cut.Find("p").TextContent));

        cut.Render(p => p.Add(c => c.ItemKey, "two"));

        cut.WaitForAssertion(() => Assert.Equal("Loaded:two", cut.Find("p").TextContent));
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, int timeoutMs = 2000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!predicate())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for expected condition.");
            }

            await Task.Delay(10);
        }
    }

    private sealed class StateFlowTestComponent : LifecycleComponentBase
    {
        private FlowState<string> _state = default!;

        [Parameter, EditorRequired]
        public IStateFlow<string> StateFlow { get; set; } = default!;

        protected override void OnInitialized()
        {
            base.OnInitialized();
            _state = CollectAsStateWithLifecycle(StateFlow);
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, _state.Value);
            builder.CloseElement();
        }
    }

    private sealed class AsyncEnumerableTestComponent : LifecycleComponentBase
    {
        private FlowState<int> _state = default!;

        [Parameter, EditorRequired]
        public IAsyncEnumerable<int> Flow { get; set; } = default!;

        [Parameter]
        public int InitialValue { get; set; }

        protected override void OnInitialized()
        {
            base.OnInitialized();
            _state = CollectAsStateWithLifecycle(Flow, InitialValue);
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, _state.Value);
            builder.CloseElement();
        }
    }

    private sealed class BackgroundJobTestComponent : LifecycleComponentBase
    {
        public Job? BackgroundJob { get; private set; }

        protected override void OnInitialized()
        {
            base.OnInitialized();
            BackgroundJob = ComponentScope.Launch(async ct =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            });
        }
    }

    private sealed class LaunchedEffectTestComponent : LifecycleComponentBase
    {
        [Parameter, EditorRequired]
        public string QueryKey { get; set; } = string.Empty;

        [Parameter]
        public Action<string>? OnStarted { get; set; }

        [Parameter]
        public Action<string>? OnCanceled { get; set; }

        protected override void OnParametersSet()
        {
            base.OnParametersSet();
            var currentKey = QueryKey;

            LaunchedEffect("search", currentKey, async ct =>
            {
                OnStarted?.Invoke(currentKey);
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                catch (OperationCanceledException)
                {
                    OnCanceled?.Invoke(currentKey);
                    throw;
                }
            });
        }
    }

    private sealed class ProduceStateTestComponent : LifecycleComponentBase
    {
        private FlowState<string> _produced = default!;

        [Parameter, EditorRequired]
        public string ItemKey { get; set; } = string.Empty;

        protected override void OnParametersSet()
        {
            base.OnParametersSet();
            var currentKey = ItemKey;

            _produced = ProduceState("item", "Loading...", currentKey, async ct =>
            {
                await Task.Delay(15, ct);
                return $"Loaded:{currentKey}";
            });
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, _produced.Value);
            builder.CloseElement();
        }
    }
}

using Bunit;
using KotlinStyleCoroutines;
using KotlinStyleCoroutines.Blazor;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Xunit;

namespace KotlinStyleCoroutines.Blazor.Tests;

public sealed class LifecycleComponentTests : BunitContext
{
    private static readonly string[] ExpectedAlpha = ["alpha"];
    private static readonly string[] ExpectedAlphaBeta = ["alpha", "beta"];

    public LifecycleComponentTests()
    {
        JSInterop.Mode = JSRuntimeMode.Loose;
    }

    [Fact]
    public void CollectAsStateWithLifecycleRendersInitialStateAndUpdatesOnStateFlowEmissions()
    {
        var stateFlow = new MutableStateFlow<string>("Idle");

        var cut = Render<StateFlowProbeComponent>(parameters => parameters
            .Add(p => p.Source, stateFlow));

        cut.MarkupMatches("<p>Idle</p>");

        stateFlow.Value = "Loading";
        cut.WaitForAssertion(() => cut.MarkupMatches("<p>Loading</p>"));

        stateFlow.Value = "Loaded: 42";
        cut.WaitForAssertion(() => cut.MarkupMatches("<p>Loaded: 42</p>"));
    }

    [Fact]
    public async Task CollectAsStateWithLifecyclePausesAndResumesOnTabVisibilityChanges()
    {
        var upstreamStarts = 0;
        var upstreamStops = 0;
        var trigger = new MutableStateFlow<int>(1);

        await using var appScope = Coroutine.CreateScope();

        IAsyncEnumerable<string> ColdFeed()
        {
            return Coroutine.ChannelFlow<string>(async (scope, writer) =>
            {
                Interlocked.Increment(ref upstreamStarts);
                try
                {
                    await foreach (var v in trigger.WithCancellation(scope.Token))
                    {
                        await writer.WriteAsync($"Tick-{v}", scope.Token);
                    }
                }
                finally
                {
                    Interlocked.Increment(ref upstreamStops);
                }
            });
        }

        var shared = ColdFeed().StateIn(
            appScope,
            SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.Zero),
            initialValue: "Init");

        var cut = Render<StateFlowProbeComponent>(parameters => parameters
            .Add(p => p.Source, shared)
            .Add(p => p.PauseWhenTabHidden, true));

        cut.WaitForAssertion(() => Assert.Equal(1, Volatile.Read(ref upstreamStarts)));
        cut.WaitForAssertion(() => cut.MarkupMatches("<p>Tick-1</p>"));

        // Simulate browser tab becoming hidden (Lifecycle STOPPED)
        await cut.InvokeAsync(() => cut.Instance.OnVisibilityChanged(false));
        cut.WaitForAssertion(() => Assert.Equal(1, Volatile.Read(ref upstreamStops)));

        // Simulate browser tab becoming visible again (Lifecycle STARTED)
        trigger.Value = 2;
        await cut.InvokeAsync(() => cut.Instance.OnVisibilityChanged(true));
        cut.WaitForAssertion(() => Assert.Equal(2, Volatile.Read(ref upstreamStarts)));
        cut.WaitForAssertion(() => cut.MarkupMatches("<p>Tick-2</p>"));
    }

    [Fact]
    public async Task ComponentDisposalCancelsActiveCollectorsAndComponentScopeJobs()
    {
        var stateFlow = new MutableStateFlow<string>("Active");
        var backgroundCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var cut = Render<DisposalProbeComponent>(parameters => parameters
            .Add(p => p.Source, stateFlow)
            .Add(p => p.OnBackgroundCanceled, () => backgroundCanceled.TrySetResult()));

        cut.MarkupMatches("<p>Active</p>");
        Assert.Equal(1, stateFlow.SubscriptionCount.Value);

        await DisposeComponentsAsync();

        await backgroundCanceled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(0, stateFlow.SubscriptionCount.Value);
    }

    [Fact]
    public void LaunchedEffectRestartsWhenKeyChangesAndCancelsPreviousWork()
    {
        var startedKeys = new List<string>();
        var canceledKeys = new List<string>();

        var cut = Render<LaunchedEffectProbeComponent>(parameters => parameters
            .Add(p => p.EffectKey, "alpha")
            .Add(p => p.OnStarted, key => startedKeys.Add(key))
            .Add(p => p.OnCanceled, key => canceledKeys.Add(key)));

        cut.WaitForAssertion(() => Assert.Equal(ExpectedAlpha, startedKeys));
        Assert.Empty(canceledKeys);

        // Re-rendering with the same key should not restart the effect
        cut.Render(parameters => parameters
            .Add(p => p.EffectKey, "alpha")
            .Add(p => p.OnStarted, key => startedKeys.Add(key))
            .Add(p => p.OnCanceled, key => canceledKeys.Add(key)));

        Assert.Single(startedKeys);

        // Changing the key cancels "alpha" and starts "beta"
        cut.Render(parameters => parameters
            .Add(p => p.EffectKey, "beta")
            .Add(p => p.OnStarted, key => startedKeys.Add(key))
            .Add(p => p.OnCanceled, key => canceledKeys.Add(key)));

        cut.WaitForAssertion(() => Assert.Equal(ExpectedAlpha, canceledKeys));
        cut.WaitForAssertion(() => Assert.Equal(ExpectedAlphaBeta, startedKeys));
    }

    [Fact]
    public void ProduceStateResolvesAsyncComputationAndUpdatesOnKeyChange()
    {
        var cut = Render<ProduceStateProbeComponent>(parameters => parameters
            .Add(p => p.Topic, "Coroutines"));

        cut.MarkupMatches("<span>Loading...</span>");
        cut.WaitForAssertion(() => cut.MarkupMatches("<span>Tip: Coroutines</span>"));

        cut.Render(parameters => parameters
            .Add(p => p.Topic, "Channels"));

        cut.WaitForAssertion(() => cut.MarkupMatches("<span>Tip: Channels</span>"));
    }

    private sealed class StateFlowProbeComponent : LifecycleComponentBase
    {
        private FlowState<string> _state = default!;

        [Parameter, EditorRequired]
        public IStateFlow<string> Source { get; set; } = default!;

        [Parameter]
        public bool PauseWhenTabHidden { get; set; }

        protected override void OnInitialized()
        {
            base.OnInitialized();
            _state = CollectAsStateWithLifecycle(Source, PauseWhenTabHidden);
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, _state.Value);
            builder.CloseElement();
        }
    }

    private sealed class DisposalProbeComponent : LifecycleComponentBase
    {
        private FlowState<string> _state = default!;

        [Parameter, EditorRequired]
        public IStateFlow<string> Source { get; set; } = default!;

        [Parameter, EditorRequired]
        public Action OnBackgroundCanceled { get; set; } = default!;

        protected override void OnInitialized()
        {
            base.OnInitialized();
            _state = CollectAsStateWithLifecycle(Source, pauseWhenTabHidden: false);

            _ = ComponentScope.Launch(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                catch (OperationCanceledException)
                {
                    OnBackgroundCanceled();
                    throw;
                }
            });
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, _state.Value);
            builder.CloseElement();
        }
    }

    private sealed class LaunchedEffectProbeComponent : LifecycleComponentBase
    {
        [Parameter, EditorRequired]
        public string EffectKey { get; set; } = string.Empty;

        [Parameter, EditorRequired]
        public Action<string> OnStarted { get; set; } = default!;

        [Parameter, EditorRequired]
        public Action<string> OnCanceled { get; set; } = default!;

        protected override void OnParametersSet()
        {
            base.OnParametersSet();
            var capturedKey = EffectKey;

            LaunchedEffect("probe-effect", capturedKey, async ct =>
            {
                OnStarted(capturedKey);
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                catch (OperationCanceledException)
                {
                    OnCanceled(capturedKey);
                    throw;
                }
            });
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "div");
            builder.AddContent(1, EffectKey);
            builder.CloseElement();
        }
    }

    private sealed class ProduceStateProbeComponent : LifecycleComponentBase
    {
        private FlowState<string> _tip = default!;

        [Parameter, EditorRequired]
        public string Topic { get; set; } = string.Empty;

        protected override void OnParametersSet()
        {
            base.OnParametersSet();

            _tip = ProduceState("produce-tip", "Loading...", Topic, async (topic, ct) =>
            {
                await Task.Delay(15, ct);
                return $"Tip: {topic}";
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

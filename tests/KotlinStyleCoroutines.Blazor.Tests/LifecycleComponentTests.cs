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
    public void CollectAsStateWithLifecycleRendersInitialAndUpdatedValues()
    {
        var stateFlow = new MutableStateFlow<string>("Initial");
        var cut = Render<StateFlowTestComponent>(p => p.Add(c => c.Source, stateFlow));

        Assert.Equal("Initial", cut.Find("p").TextContent);

        stateFlow.Value = "Updated";

        cut.WaitForAssertion(() => Assert.Equal("Updated", cut.Find("p").TextContent));
    }

    [Fact]
    public async Task CollectAsStateWithLifecyclePausesAndResumesOnTabVisibility()
    {
        var upstreamStarts = 0;
        var upstreamStops = 0;

        async IAsyncEnumerable<int> ColdSensor([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            upstreamStarts++;
            try
            {
                var i = 1;
                while (!ct.IsCancellationRequested)
                {
                    yield return i++;
                    await Task.Delay(20, ct);
                }
            }
            finally
            {
                upstreamStops++;
            }
        }

        await using var scope = Coroutine.CreateScope();
        var sharedState = ColdSensor().StateIn(
            scope,
            SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.Zero),
            initialValue: 0);

        var cut = Render<IntStateFlowTestComponent>(p => p.Add(c => c.Source, sharedState));

        cut.WaitForAssertion(() => Assert.True(Volatile.Read(ref upstreamStarts) >= 1));
        cut.WaitForAssertion(() => Assert.NotEqual("0", cut.Find("p").TextContent));

        // Simulate browser tab hidden -> pauses collection -> WhileSubscribed stops upstream
        await cut.InvokeAsync(() => cut.Instance.OnVisibilityChanged(false));
        cut.WaitForAssertion(() => Assert.Equal(1, Volatile.Read(ref upstreamStops)));

        // Simulate browser tab visible -> resumes collection -> WhileSubscribed restarts upstream
        await cut.InvokeAsync(() => cut.Instance.OnVisibilityChanged(true));
        cut.WaitForAssertion(() => Assert.Equal(2, Volatile.Read(ref upstreamStarts)));
    }

    [Fact]
    public async Task DisposeComponentsAsyncCancelsComponentScopeJobs()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var cut = Render<BackgroundJobTestComponent>(p => p
            .Add(c => c.OnStarted, started)
            .Add(c => c.OnCanceled, canceled));

        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // Capture Job reference before disposing the component, because bUnit blocks cut.Instance after disposal
        var backgroundJob = cut.Instance.BackgroundJob;
        Assert.NotNull(backgroundJob);

        await DisposeComponentsAsync();

        await canceled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(backgroundJob.IsCanceled);
        Assert.True(backgroundJob.IsCompleted);
    }

    [Fact]
    public void LaunchedEffectRestartsWhenKeyChanges()
    {
        var startedKeys = new List<string>();
        var canceledKeys = new List<string>();

        var cut = Render<LaunchedEffectTestComponent>(p => p
            .Add(c => c.ItemKey, "alpha")
            .Add(c => c.StartedKeys, startedKeys)
            .Add(c => c.CanceledKeys, canceledKeys));

        cut.WaitForAssertion(() => Assert.Contains("alpha", startedKeys));
        Assert.Empty(canceledKeys);

        cut.Render(p => p
            .Add(c => c.ItemKey, "beta")
            .Add(c => c.StartedKeys, startedKeys)
            .Add(c => c.CanceledKeys, canceledKeys));

        cut.WaitForAssertion(() => Assert.Contains("alpha", canceledKeys));
        cut.WaitForAssertion(() => Assert.Contains("beta", startedKeys));
    }

    [Fact]
    public void ProduceStateResolvesAsyncValueAndUpdatesWhenKeyChanges()
    {
        var cut = Render<ProduceStateTestComponent>(p => p.Add(c => c.QueryKey, "first"));

        cut.WaitForAssertion(() => Assert.Equal("Result:first", cut.Find("span").TextContent));

        cut.Render(p => p.Add(c => c.QueryKey, "second"));

        cut.WaitForAssertion(() => Assert.Equal("Result:second", cut.Find("span").TextContent));
    }

    private sealed class StateFlowTestComponent : LifecycleComponentBase
    {
        private FlowState<string> _state = default!;

        [Parameter, EditorRequired]
        public IStateFlow<string> Source { get; set; } = default!;

        protected override void OnInitialized()
        {
            base.OnInitialized();
            _state = CollectAsStateWithLifecycle(Source);
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, _state.Value);
            builder.CloseElement();
        }
    }

    private sealed class IntStateFlowTestComponent : LifecycleComponentBase
    {
        private FlowState<int> _state = default!;

        [Parameter, EditorRequired]
        public IStateFlow<int> Source { get; set; } = default!;

        protected override void OnInitialized()
        {
            base.OnInitialized();
            _state = CollectAsStateWithLifecycle(Source);
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
        [Parameter, EditorRequired]
        public TaskCompletionSource OnStarted { get; set; } = default!;

        [Parameter, EditorRequired]
        public TaskCompletionSource OnCanceled { get; set; } = default!;

        public Job? BackgroundJob { get; private set; }

        protected override void OnInitialized()
        {
            base.OnInitialized();
            BackgroundJob = ComponentScope.Launch(async ct =>
            {
                OnStarted.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                catch (OperationCanceledException)
                {
                    OnCanceled.TrySetResult();
                    throw;
                }
            });
        }
    }

    private sealed class LaunchedEffectTestComponent : LifecycleComponentBase
    {
        [Parameter, EditorRequired]
        public string ItemKey { get; set; } = string.Empty;

        [Parameter, EditorRequired]
        public List<string> StartedKeys { get; set; } = default!;

        [Parameter, EditorRequired]
        public List<string> CanceledKeys { get; set; } = default!;

        protected override void OnParametersSet()
        {
            base.OnParametersSet();
            var currentKey = ItemKey;

            LaunchedEffect("item-loader", currentKey, async ct =>
            {
                lock (StartedKeys)
                {
                    StartedKeys.Add(currentKey);
                }

                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
                catch (OperationCanceledException)
                {
                    lock (CanceledKeys)
                    {
                        CanceledKeys.Add(currentKey);
                    }
                    throw;
                }
            });
        }
    }

    private sealed class ProduceStateTestComponent : LifecycleComponentBase
    {
        private FlowState<string> _produced = default!;

        [Parameter, EditorRequired]
        public string QueryKey { get; set; } = string.Empty;

        protected override void OnParametersSet()
        {
            base.OnParametersSet();
            var currentKey = QueryKey;

            _produced = ProduceState("query-producer", "Loading", currentKey, async ct =>
            {
                await Task.Delay(10, ct);
                return $"Result:{currentKey}";
            });
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "span");
            builder.AddContent(1, _produced.Value);
            builder.CloseElement();
        }
    }
}

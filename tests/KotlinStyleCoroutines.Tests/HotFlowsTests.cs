using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using KotlinStyleCoroutines;
using Xunit;

namespace KotlinStyleCoroutines.Tests;

public class HotFlowsTests
{
    private static readonly int[] ExpectedStateValues = [15, 20, 30];
    private static readonly string[] ExpectedInitialReplay = ["event-2", "event-3"];
    private static readonly string[] ExpectedBroadcastEvents = ["event-2", "event-3", "event-4"];

    [Fact]
    public async Task StateFlowReplaysCurrentValueDeduplicatesAndConflates()
    {
        var state = new MutableStateFlow<int>(initialValue: 10);
        Assert.Equal(10, state.Value);

        // Duplicate value is ignored
        Assert.False(state.TryEmit(10));

        // Atomic update
        int updated = state.UpdateAndGet(x => x + 5);
        Assert.Equal(15, updated);

        var received = new List<int>();
        using var cts = new CancellationTokenSource();

        await Coroutine.Scope(async scope =>
        {
            var collector = state.LaunchIn(scope, val =>
            {
                received.Add(val);
                if (val == 30)
                    cts.Cancel();
            });

            await Task.Delay(30);
            state.Value = 20;
            await Task.Delay(30);
            state.Value = 30;

            await collector.JoinAsync();
        }, cts.Token);

        Assert.Equal(ExpectedStateValues, received);
    }

    [Fact]
    public async Task SharedFlowReplaysConfiguredHistoryAndBroadcastsToMultipleSubscribers()
    {
        var events = new MutableSharedFlow<string>(replay: 2, extraBufferCapacity: 4);

        // Emit before any subscribers attach
        await events.EmitAsync("event-1");
        await events.EmitAsync("event-2");
        await events.EmitAsync("event-3");

        Assert.Equal(ExpectedInitialReplay, events.ReplayCache);

        var sub1 = new List<string>();
        var sub2 = new List<string>();
        using var cts = new CancellationTokenSource();

        await Coroutine.Scope(async scope =>
        {
            var j1 = events.LaunchIn(scope, e => sub1.Add(e));
            var j2 = events.LaunchIn(scope, e =>
            {
                sub2.Add(e);
                if (sub2.Count == 3)
                    cts.Cancel();
            });

            while (events.SubscriptionCount.Value < 2)
                await Task.Delay(10);

            await Task.Delay(30);
            await events.EmitAsync("event-4");

            await j1.JoinAsync();
            await j2.JoinAsync();
        }, cts.Token);

        Assert.Equal(ExpectedBroadcastEvents, sub1);
        Assert.Equal(ExpectedBroadcastEvents, sub2);
    }

    [Fact]
    public async Task ShareInWhileSubscribedStartsOnFirstSubscriberAndStopsWhenLastUnsubscribes()
    {
        int upstreamStarts = 0;

        async IAsyncEnumerable<int> ColdSensor([EnumeratorCancellation] CancellationToken ct = default)
        {
            Interlocked.Increment(ref upstreamStarts);
            int i = 0;
            while (!ct.IsCancellationRequested)
            {
                yield return ++i;
                await Task.Delay(20, ct);
            }
        }

        using var scopeCts = new CancellationTokenSource();

        await Coroutine.Scope(async scope =>
        {
            var hot = ColdSensor().ShareIn(
                scope,
                SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.Zero),
                replay: 1);

            // Upstream must not start until a subscriber attaches
            await Task.Delay(40);
            Assert.Equal(0, upstreamStarts);

            // 1. First subscriber attaches -> starts upstream
            var firstCollected = new List<int>();
            var sub1Job = scope.Launch(async ct =>
            {
                await foreach (var item in hot.WithCancellation(ct))
                {
                    firstCollected.Add(item);
                    if (firstCollected.Count >= 2)
                        break;
                }
            });

            await sub1Job.JoinAsync();
            Assert.Equal(1, upstreamStarts);
            Assert.True(firstCollected.Count >= 2);

            // Wait for SubscriptionCount to drop back to 0 so WhileSubscribed stops upstream
            while (hot.SubscriptionCount.Value > 0)
                await Task.Delay(10);

            await Task.Delay(30);

            // 2. Second subscriber attaches -> restarts upstream
            var secondCollected = new List<int>();
            var sub2Job = scope.Launch(async ct =>
            {
                await foreach (var item in hot.WithCancellation(ct))
                {
                    secondCollected.Add(item);
                    if (secondCollected.Count >= 2)
                        break;
                }
            });

            await sub2Job.JoinAsync();
            Assert.Equal(2, upstreamStarts);

            // Cancel the scope via its linked outer token so LaunchSharing exits cleanly
            await scopeCts.CancelAsync();
        }, scopeCts.Token);
    }
}

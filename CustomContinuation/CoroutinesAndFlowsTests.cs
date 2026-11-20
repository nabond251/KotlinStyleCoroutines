using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KotlinStyleCoroutines;
using Xunit;

namespace KotlinStyleCoroutines.Tests;

public class CoroutinesAndFlowsTests
{
    [Fact]
    public async Task SuspendCancellable_ResumesWithValue_AndHandlesCancellation()
    {
        var result = await Coroutine.SuspendCancellable<string>(cont =>
        {
            Task.Run(() => cont.Resume("hello-kotlin"));
        });

        Assert.Equal("hello-kotlin", result);

        using var cts = new CancellationTokenSource();
        bool cancelCallbackInvoked = false;

        var pending = Coroutine.SuspendCancellable<int>(cont =>
        {
            cont.InvokeOnCancellation(() => cancelCallbackInvoked = true);
        }, cts.Token);

        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () => await pending);
        Assert.True(cancelCallbackInvoked);
    }

    [Fact]
    public async Task CoroutineScope_CancelsSiblingsWhenOneChildThrows()
    {
        bool siblingCanceled = false;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Coroutine.Scope(async scope =>
            {
                scope.Launch(async ct =>
                {
                    try
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        siblingCanceled = true;
                        throw;
                    }
                });

                await Task.Delay(30, scope.Token);
                throw new InvalidOperationException("Boom");
            });
        });

        Assert.True(siblingCanceled);
    }

    [Fact]
    public async Task SupervisorScope_IsolatesChildFailures()
    {
        Exception? captured = null;
        bool healthyCompleted = false;

        await Coroutine.SupervisorScope(async scope =>
        {
            scope.Launch(async ct =>
            {
                await Task.Delay(20, ct);
                throw new ApplicationException("Isolated failure");
            });

            var healthyJob = scope.Launch(async ct =>
            {
                await Task.Delay(60, ct);
                healthyCompleted = true;
            });

            await healthyJob.JoinAsync();
        }, onUnhandledChildException: ex => captured = ex);

        Assert.IsType<ApplicationException>(captured);
        Assert.True(healthyCompleted);
    }

    [Fact]
    public async Task Job_CanBeCanceledAndJoinedIndividually()
    {
        await Coroutine.Scope(async scope =>
        {
            int ticks = 0;
            Job ticker = scope.Launch(async ct =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Interlocked.Increment(ref ticks);
                    await Task.Delay(20, ct);
                }
            });

            await Task.Delay(55, scope.Token);
            ticker.Cancel();
            await ticker.JoinAsync();

            Assert.True(ticker.IsCanceled);
            Assert.True(ticks >= 2);
            Assert.False(scope.Token.IsCancellationRequested);
        });
    }

    [Fact]
    public async Task StateFlow_ReplaysCurrentValue_Deduplicates_AndConflates()
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

        Assert.Equal(new[] { 15, 20, 30 }, received);
    }

    [Fact]
    public async Task SharedFlow_ReplaysConfiguredHistory_AndBroadcastsToMultipleSubscribers()
    {
        var events = new MutableSharedFlow<string>(replay: 2, extraBufferCapacity: 4);

        await events.EmitAsync("event-1");
        await events.EmitAsync("event-2");
        await events.EmitAsync("event-3"); // Evicts event-1 from replay=2

        Assert.Equal(new[] { "event-2", "event-3" }, events.ReplayCache);

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

            // Wait until both subscribers have registered and consumed replay
            while (events.SubscriptionCount.Value < 2)
                await Task.Delay(10);

            await Task.Delay(30);
            await events.EmitAsync("event-4");

            await j1.JoinAsync();
            await j2.JoinAsync();
        }, cts.Token);

        Assert.Equal(new[] { "event-2", "event-3", "event-4" }, sub1);
        Assert.Equal(new[] { "event-2", "event-3", "event-4" }, sub2);
    }

    [Fact]
    public async Task ShareIn_WhileSubscribed_StartsOnFirstSubscriber_AndStopsWhenLastUnsubscribes()
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

        await Coroutine.Scope(async scope =>
        {
            var hot = ColdSensor().ShareIn(
                scope,
                SharingStarted.WhileSubscribed(stopTimeout: TimeSpan.Zero),
                replay: 1);

            await Task.Delay(40);
            // Lazily started: upstream has not run yet because SubscriptionCount == 0
            Assert.Equal(0, upstreamStarts);

            using var sub1Cts = new CancellationTokenSource();
            var firstCollected = new List<int>();

            var sub1Job = scope.Launch(async _ =>
            {
                await foreach (var item in hot.WithCancellation(sub1Cts.Token))
                {
                    firstCollected.Add(item);
                    if (firstCollected.Count >= 2)
                        sub1Cts.Cancel();
                }
            });

            await sub1Job.JoinAsync();
            Assert.Equal(1, upstreamStarts);
            Assert.True(firstCollected.Count >= 2);

            // Allow WhileSubscribed to stop upstream after subscriber count drops to 0
            await Task.Delay(40);
            Assert.Equal(0, hot.SubscriptionCount.Value);

            // Second subscriber restarts upstream cleanly
            using var sub2Cts = new CancellationTokenSource();
            var secondCollected = new List<int>();
            var sub2Job = scope.Launch(async _ =>
            {
                await foreach (var item in hot.WithCancellation(sub2Cts.Token))
                {
                    secondCollected.Add(item);
                    if (secondCollected.Count >= 2)
                        sub2Cts.Cancel();
                }
            });

            await sub2Job.JoinAsync();
            Assert.Equal(2, upstreamStarts);
            scope.Cancel();
        });
    }

    [Fact]
    public void LuaCoroutine_ExchangesValuesTwoWays_UntilDead()
    {
        static async LuaCoroutine<string, int> StateMachine()
        {
            string a = await Lua.Yield<string, int>(100);
            Assert.Equal("step-1", a);

            string b = await Lua.Yield<string, int>(200);
            Assert.Equal("step-2", b);

            return 999;
        }

        var co = StateMachine();
        Assert.False(co.IsDead);

        var r1 = co.Resume("ignored-initial");
        Assert.Equal((true, 100), r1);
        Assert.False(co.IsDead);

        var r2 = co.Resume("step-1");
        Assert.Equal((true, 200), r2);
        Assert.False(co.IsDead);

        var r3 = co.Resume("step-2");
        Assert.Equal((true, 999), r3);
        Assert.True(co.IsDead);

        var r4 = co.Resume("after-death");
        Assert.Equal((false, 0), r4);
    }
}

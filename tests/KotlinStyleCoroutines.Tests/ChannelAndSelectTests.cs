using System.Threading.Channels;
using Xunit;

namespace KotlinStyleCoroutines.Tests;

public sealed class ChannelAndSelectTests
{
    private static readonly int[] ExpectedProducedItems = [10, 20, 30];

    [Fact]
    public async Task ProduceStreamsElementsAndCompletesChannelAutomatically()
    {
        var received = new List<int>();

        await Coroutine.Scope(async scope =>
        {
            var numbers = scope.Produce<int>(async producer =>
            {
                await producer.SendAsync(10, producer.Token);
                await producer.SendAsync(20, producer.Token);
                await producer.SendAsync(30, producer.Token);
            }, capacity: 2);

            await foreach (var item in numbers.ReadAllAsync(scope.Token))
            {
                received.Add(item);
            }

            await numbers.JoinAsync();
        });

        Assert.Equal(ExpectedProducedItems, received);
    }

    [Fact]
    public async Task ActorProcessesMailboxMessagesSequentiallyAndCompletesOnClose()
    {
        var finalCount = 0;

        await Coroutine.Scope(async scope =>
        {
            var counter = scope.Actor<int>(async actor =>
            {
                var total = 0;
                await foreach (var delta in actor.ReadAllAsync(actor.Token))
                {
                    total += delta;
                }

                Volatile.Write(ref finalCount, total);
            }, capacity: 8);

            await counter.SendAsync(5, scope.Token);
            await counter.SendAsync(12, scope.Token);
            await counter.SendAsync(25, scope.Token);
            counter.Close();

            await counter.JoinAsync();
        });

        Assert.Equal(42, finalCount);
    }

    [Fact]
    public async Task SelectPicksFastestChannelWithoutLosingItemsFromOtherChannel()
    {
        await Coroutine.Scope(async scope =>
        {
            var fast = Channel.CreateUnbounded<string>();
            var slow = Channel.CreateUnbounded<string>();

            await fast.Writer.WriteAsync("fast-1", scope.Token);
            await slow.Writer.WriteAsync("slow-1", scope.Token);

            // Force slow to wait so fast wins deterministically
            var delayedSlow = scope.Produce<string>(async p =>
            {
                await Coroutine.Delay(60, p.Token);
                await p.SendAsync("delayed-slow", p.Token);
            });

            var firstWinner = await Coroutine.Select<string>(s =>
            {
                s.OnReceive(fast.Reader, val => $"Fast:{val}");
                s.OnReceive(delayedSlow, val => $"Slow:{val}");
            });

            Assert.Equal("Fast:fast-1", firstWinner);

            // Verify delayedSlow did not lose its item when it lost the first select race
            var secondWinner = await Coroutine.Select<string>(s =>
            {
                s.OnReceive(fast.Reader, val => $"Fast:{val}");
                s.OnReceive(delayedSlow, val => $"Slow:{val}");
            });

            Assert.Equal("Slow:delayed-slow", secondWinner);
            delayedSlow.Cancel();
        });
    }

    [Fact]
    public async Task SelectSupportsDeferredAwaitAndTimeoutClauses()
    {
        await Coroutine.Scope(async scope =>
        {
            var slowDeferred = scope.Async(async ct =>
            {
                await Task.Delay(500, ct);
                return "too-late";
            });

            var timedOutResult = await Coroutine.Select<string>(s =>
            {
                s.OnAwait(slowDeferred, val => val);
                s.OnTimeout(TimeSpan.FromMilliseconds(25), () => "timed-out");
            });

            Assert.Equal("timed-out", timedOutResult);
            slowDeferred.Cancel();

            var fastDeferred = scope.Async(async ct =>
            {
                await Task.Delay(10, ct);
                return 99;
            });

            var wonResult = await Coroutine.Select<int>(s =>
            {
                s.OnAwait(fastDeferred, val => val + 1);
                s.OnTimeout(TimeSpan.FromSeconds(5), () => -1);
            });

            Assert.Equal(100, wonResult);
        });
    }

    [Fact]
    public async Task SelectOnReceiveCatchingHandlesClosedChannelsGracefully()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryComplete();

        var outcome = await Coroutine.Select<string>(s =>
        {
            s.OnReceiveCatching(channel.Reader, result =>
                result.IsClosed ? "channel-closed" : $"got-{result.Value}");
        });

        Assert.Equal("channel-closed", outcome);
    }
}

using System.Threading.Channels;
using Xunit;

namespace KotlinStyleCoroutines.Tests;

public sealed class ChannelAndSelectTests
{
    private static readonly int[] ExpectedProducedItems = [10, 20, 30];

    [Fact]
    public async Task ProduceStreamsElementsAndClosesChannelWhenBlockCompletes()
    {
        var collected = new List<int>();

        await Coroutine.Scope(async scope =>
        {
            var numbers = scope.Produce<int>(async producer =>
            {
                await producer.SendAsync(10);
                await producer.SendAsync(20);
                await producer.SendAsync(30);
            }, capacity: 2);

            await foreach (var n in numbers)
            {
                collected.Add(n);
            }

            Assert.True(numbers.Reader.Completion.IsCompletedSuccessfully);
        });

        Assert.Equal(ExpectedProducedItems, collected);
    }

    [Fact]
    public async Task ActorProcessesMessagesSequentiallyInMailbox()
    {
        var finalCount = 0;

        await Coroutine.Scope(async scope =>
        {
            var counter = scope.Actor<int>(async actor =>
            {
                var total = 0;
                await foreach (var delta in actor.ReadAllAsync())
                {
                    total += delta;
                }
                finalCount = total;
            }, capacity: 16);

            for (var i = 1; i <= 5; i++)
            {
                await counter.SendAsync(i);
            }

            counter.Close();
            await counter.Job.JoinAsync();
        });

        Assert.Equal(15, finalCount);
    }

    [Fact]
    public async Task SelectRacesChannelsWithoutDroppingLosingChannelItems()
    {
        var fast = Channel.CreateUnbounded<string>();
        var slow = Channel.CreateUnbounded<string>();

        await slow.Writer.WriteAsync("from-slow");
        await fast.Writer.WriteAsync("from-fast");

        // Drain fast first, then verify slow's item was never consumed by the losing clause
        var first = await Coroutine.Select<string>(s =>
        {
            s.OnReceive(fast.Reader, val => $"Fast:{val}");
        });

        var second = await Coroutine.Select<string>(s =>
        {
            s.OnReceive(fast.Reader, val => $"Fast:{val}");
            s.OnReceive(slow.Reader, val => $"Slow:{val}");
        });

        Assert.Equal("Fast:from-fast", first);
        Assert.Equal("Slow:from-slow", second);
    }

    [Fact]
    public async Task SelectSupportsDeferredAndOnTimeout()
    {
        await Coroutine.Scope(async scope =>
        {
            var slowWork = scope.Async(async ct =>
            {
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                return "done";
            });

            var outcome = await Coroutine.Select<string>(s =>
            {
                s.OnAwait(slowWork, val => val);
                s.OnTimeout(TimeSpan.FromMilliseconds(25), () => "timed-out");
            });

            Assert.Equal("timed-out", outcome);
            slowWork.Cancel();
        });
    }

    [Fact]
    public async Task SelectOnReceiveCatchingReportsClosedChannel()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryComplete();

        var status = await Coroutine.Select<string>(s =>
        {
            s.OnReceiveCatching(channel.Reader, res => res.IsClosed ? "closed" : $"value:{res.GetOrNull()}");
        });

        Assert.Equal("closed", status);
    }
}

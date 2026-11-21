using System;
using System.Threading;
using System.Threading.Tasks;
using KotlinStyleCoroutines;
using Xunit;

namespace KotlinStyleCoroutines.Tests;

public class StructuredConcurrencyTests
{
    [Fact]
    public async Task SuspendCancellableResumesWithValueAndHandlesCancellation()
    {
        var result = await Coroutine.SuspendCancellable<string>(cont =>
        {
            _ = Task.Run(() => cont.Resume("hello-kotlin"));
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
    public async Task CoroutineScopeCancelsSiblingsWhenOneChildThrows()
    {
        bool siblingCanceled = false;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Coroutine.Scope(async scope =>
            {
                _ = scope.Launch(async ct =>
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
    public async Task SupervisorScopeIsolatesChildFailures()
    {
        Exception? captured = null;
        bool healthyCompleted = false;

        await Coroutine.SupervisorScope(async scope =>
        {
            _ = scope.Launch(async ct =>
            {
                await Task.Delay(20, ct);
                throw new InvalidOperationException("Isolated failure");
            });

            var healthyJob = scope.Launch(async ct =>
            {
                await Task.Delay(60, ct);
                healthyCompleted = true;
            });

            await healthyJob.JoinAsync();
        }, onUnhandledChildException: ex => captured = ex);

        Assert.IsType<InvalidOperationException>(captured);
        Assert.True(healthyCompleted);
    }

    [Fact]
    public async Task JobCanBeCanceledAndJoinedIndividually()
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
}

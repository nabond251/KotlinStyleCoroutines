using Xunit;

namespace KotlinStyleCoroutines.Tests;

public class StructuredConcurrencyTests
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
}

using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace KotlinStyleCoroutines;

public static partial class Coroutine
{
    public static Suspendable<T> SuspendCancellable<T>(
        Action<Continuation<T>> block,
        CancellationToken token = default)
        => new(block, ResolveToken(token));

    /// <summary>
    /// Creates a standalone <see cref="CoroutineScope"/> linked to <paramref name="parentToken"/>
    /// (or automatically linked to <see cref="CurrentToken"/> when <paramref name="parentToken"/> is <see langword="default"/>).
    /// </summary>
    public static CoroutineScope CreateScope(CancellationToken parentToken = default)
        => new(ResolveToken(parentToken));

    // Replicates Kotlin's `coroutineScope { ... }` suspending block.
    // Automatically links to any enclosing Coroutine.CurrentToken when cancellationToken is default.
    public static async Task<T> Scope<T>(
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        var effectiveParent = ResolveToken(cancellationToken);
        await using var scope = new CoroutineScope(effectiveParent);
        var previous = SwapAmbientToken(scope.Token);
        try
        {
            return await block(scope).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !effectiveParent.IsCancellationRequested)
        {
            // If the main scope body throws, cancel all background children before DisposeAsync awaits them
            await scope.CancelAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            RestoreAmbientToken(previous);
        }
    }

    public static async Task Scope(
        Func<CoroutineScope, Task> block,
        CancellationToken cancellationToken = default)
    {
        await Scope<object?>(async s =>
        {
            await block(s).ConfigureAwait(false);
            return null;
        }, cancellationToken).ConfigureAwait(false);
    }

    // Replicates Kotlin's `supervisorScope { ... }`:
    // Child failures do not cancel sibling coroutines or fail the parent scope unless thrown directly from `block`.
    public static async Task<T> SupervisorScope<T>(
        Func<CoroutineScope, Task<T>> block,
        Action<Exception>? onUnhandledChildException = null,
        CancellationToken cancellationToken = default)
    {
        var effectiveParent = ResolveToken(cancellationToken);
        await using var scope = new CoroutineScope(effectiveParent, isSupervisor: true, onUnhandledChildException);
        var previous = SwapAmbientToken(scope.Token);
        try
        {
            return await block(scope).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !effectiveParent.IsCancellationRequested)
        {
            await scope.CancelAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            RestoreAmbientToken(previous);
        }
    }

    public static async Task SupervisorScope(
        Func<CoroutineScope, Task> block,
        Action<Exception>? onUnhandledChildException = null,
        CancellationToken cancellationToken = default)
    {
        await SupervisorScope<object?>(async s =>
        {
            await block(s).ConfigureAwait(false);
            return null;
        }, onUnhandledChildException, cancellationToken).ConfigureAwait(false);
    }

    // Scoped WithContext overloads for TaskScheduler (e.g., Dispatchers.Default, LimitedParallelism, SingleThread)
    public static Task<T> WithContext<T>(
        TaskScheduler scheduler,
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        var effectiveParent = ResolveToken(cancellationToken);
        effectiveParent.ThrowIfCancellationRequested();

        // Task.Factory.StartNew sets TaskScheduler.Current for the entire async execution tree inside `block`
        return Task.Factory.StartNew(
            () => Scope(block, effectiveParent),
            effectiveParent,
            TaskCreationOptions.DenyChildAttach,
            scheduler).Unwrap();
    }

    public static Task WithContext(
        TaskScheduler scheduler,
        Func<CoroutineScope, Task> block,
        CancellationToken cancellationToken = default)
    {
        return WithContext<object?>(scheduler, async scope =>
        {
            await block(scope).ConfigureAwait(false);
            return null;
        }, cancellationToken);
    }

    // Scoped WithContext overloads for SynchronizationContext (e.g., Dispatchers.Main)
    public static async Task<T> WithContext<T>(
        SynchronizationContext targetContext,
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        var effectiveParent = ResolveToken(cancellationToken);
        effectiveParent.ThrowIfCancellationRequested();

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        targetContext.Post(async static state =>
        {
            var (fn, ct, source) = ((Func<CoroutineScope, Task<T>>, CancellationToken, TaskCompletionSource<T>))state!;
            try
            {
                ct.ThrowIfCancellationRequested();
                var result = await Scope(fn, ct);
                source.TrySetResult(result);
            }
            catch (OperationCanceledException oce) when (ct.IsCancellationRequested)
            {
                source.TrySetCanceled(oce.CancellationToken);
            }
            catch (Exception ex)
            {
                source.TrySetException(ex);
            }
        }, (block, effectiveParent, tcs));

        return await tcs.Task.ConfigureAwait(false);
    }

    public static Task WithContext(
        SynchronizationContext targetContext,
        Func<CoroutineScope, Task> block,
        CancellationToken cancellationToken = default)
    {
        return WithContext<object?>(targetContext, async scope =>
        {
            await block(scope).ConfigureAwait(false);
            return null;
        }, cancellationToken);
    }

    // ========================================================================
    // PART 3: Kotlin Flow Bridges, Hot SharedFlow<T>, StateFlow<T>, & Operators
    // ========================================================================

    public static async IAsyncEnumerable<T> ChannelFlow<T>(
        Func<CoroutineScope, ChannelWriter<T>, Task> block,
        int capacity = 64,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var effectiveParent = ResolveToken(cancellationToken);
        var channel = Channel.CreateBounded<T>(capacity);
        using var consumerCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveParent);

        var producerTask = Scope(async scope =>
        {
            await block(scope, channel.Writer).ConfigureAwait(false);
        }, consumerCts.Token);

        _ = producerTask.ContinueWith(
            static (t, state) =>
            {
                var writer = (ChannelWriter<T>)state!;
                if (t.IsFaulted)
                    writer.TryComplete(t.Exception?.InnerException ?? t.Exception);
                else
                    writer.TryComplete();
            },
            channel.Writer,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(effectiveParent).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await consumerCts.CancelAsync().ConfigureAwait(false);
            try
            {
                await producerTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (consumerCts.IsCancellationRequested)
            {
            }
        }
    }

    public static IAsyncEnumerable<T> Merge<T>(params IAsyncEnumerable<T>[] flows)
    {
        return ChannelFlow<T>(async (scope, writer) =>
        {
            foreach (var flow in flows)
            {
                var currentFlow = flow;
                _ = scope.Launch(async ct =>
                {
                    await foreach (var item in currentFlow.WithCancellation(ct).ConfigureAwait(false))
                    {
                        await writer.WriteAsync(item, ct).ConfigureAwait(false);
                    }
                });
            }

            await Task.CompletedTask;
        });
    }

    public static async IAsyncEnumerable<TResult> Combine<T1, T2, TResult>(
        IAsyncEnumerable<T1> flow1,
        IAsyncEnumerable<T2> flow2,
        Func<T1, T2, TResult> transform,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var effectiveParent = ResolveToken(cancellationToken);
        var sync = new Lock();
        var has1 = false;
        var has2 = false;
        T1 latest1 = default!;
        T2 latest2 = default!;

        var signal = Channel.CreateUnbounded<TResult>();
        using var consumerCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveParent);

        var producerTask = Scope(async scope =>
        {
            var j1 = scope.Launch(async ct =>
            {
                await foreach (var v1 in flow1.WithCancellation(ct).ConfigureAwait(false))
                {
                    TResult? combined = default;
                    bool shouldEmit = false;
                    lock (sync)
                    {
                        latest1 = v1;
                        has1 = true;
                        if (has2)
                        {
                            combined = transform(latest1, latest2);
                            shouldEmit = true;
                        }
                    }

                    if (shouldEmit)
                        signal.Writer.TryWrite(combined!);
                }
            });

            var j2 = scope.Launch(async ct =>
            {
                await foreach (var v2 in flow2.WithCancellation(ct).ConfigureAwait(false))
                {
                    TResult? combined = default;
                    bool shouldEmit = false;
                    lock (sync)
                    {
                        latest2 = v2;
                        has2 = true;
                        if (has1)
                        {
                            combined = transform(latest1, latest2);
                            shouldEmit = true;
                        }
                    }

                    if (shouldEmit)
                        signal.Writer.TryWrite(combined!);
                }
            });

            await j1.JoinAsync().ConfigureAwait(false);
            await j2.JoinAsync().ConfigureAwait(false);
        }, consumerCts.Token);

        _ = producerTask.ContinueWith(
            static (t, state) =>
            {
                var writer = (ChannelWriter<TResult>)state!;
                if (t.IsFaulted)
                    writer.TryComplete(t.Exception?.InnerException ?? t.Exception);
                else
                    writer.TryComplete();
            },
            signal.Writer,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        try
        {
            await foreach (var item in signal.Reader.ReadAllAsync(effectiveParent).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await consumerCts.CancelAsync().ConfigureAwait(false);
            try
            {
                await producerTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (consumerCts.IsCancellationRequested)
            {
            }
        }
    }

    public static IAsyncEnumerable<TResult> Combine<T1, T2, T3, TResult>(
        IAsyncEnumerable<T1> flow1,
        IAsyncEnumerable<T2> flow2,
        IAsyncEnumerable<T3> flow3,
        Func<T1, T2, T3, TResult> transform)
    {
        return Combine(
            Combine(flow1, flow2, static (a, b) => (a, b)),
            flow3,
            (pair, c) => transform(pair.a, pair.b, c));
    }

    public static async IAsyncEnumerable<TResult> Zip<T1, T2, TResult>(
        IAsyncEnumerable<T1> first,
        IAsyncEnumerable<T2> second,
        Func<T1, T2, TResult> transform,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var effectiveParent = ResolveToken(cancellationToken);
        await using var e1 = first.GetAsyncEnumerator(effectiveParent);
        await using var e2 = second.GetAsyncEnumerator(effectiveParent);

        while (await e1.MoveNextAsync().ConfigureAwait(false) &&
               await e2.MoveNextAsync().ConfigureAwait(false))
        {
            yield return transform(e1.Current, e2.Current);
        }
    }
}

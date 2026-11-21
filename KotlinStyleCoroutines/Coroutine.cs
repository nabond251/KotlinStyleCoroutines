using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Job, Deferred<T>, Dispatchers, & Structured Concurrency
/// </summary>
public static partial class Coroutine
{
    public static Suspendable<T> SuspendCancellable<T>(
        Action<Continuation<T>> block,
        CancellationToken token = default)
        => new(block, token);

    public static async Task<T> Scope<T>(
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        await using var scope = new CoroutineScope(cancellationToken, isSupervisor: false);
        try
        {
            return await block(scope).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            await scope.CancelAsync().ConfigureAwait(false);
            throw;
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

    public static async Task<T> SupervisorScope<T>(
        Func<CoroutineScope, Task<T>> block,
        Action<Exception>? onUnhandledChildException = null,
        CancellationToken cancellationToken = default)
    {
        await using var scope = new CoroutineScope(
            cancellationToken,
            isSupervisor: true,
            onChildException: onUnhandledChildException);

        try
        {
            return await block(scope).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            await scope.CancelAsync().ConfigureAwait(false);
            throw;
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

    public static Task<T> WithContext<T>(
        TaskScheduler scheduler,
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return Task.Factory.StartNew(
            () => Scope(block, cancellationToken),
            cancellationToken,
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

    public static async Task<T> WithContext<T>(
        SynchronizationContext targetContext,
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

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
        }, (block, cancellationToken, tcs));

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
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        using var consumerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Scope(async scope =>
        {
            try
            {
                await block(scope, channel.Writer).ConfigureAwait(false);
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }, consumerCts.Token);

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(consumerCts.Token).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await consumerCts.CancelAsync().ConfigureAwait(false);
        }

        await producerTask.ConfigureAwait(false);
    }

    public static IAsyncEnumerable<T> Merge<T>(params IAsyncEnumerable<T>[] flows)
        => Merge((IEnumerable<IAsyncEnumerable<T>>)flows);

    public static IAsyncEnumerable<T> Merge<T>(
        IEnumerable<IAsyncEnumerable<T>> flows,
        int capacity = 64)
    {
        var flowList = flows as IReadOnlyList<IAsyncEnumerable<T>> ?? flows.ToList();
        return ChannelFlow<T>((scope, writer) =>
        {
            foreach (var flow in flowList)
            {
                scope.Launch(async ct =>
                {
                    await foreach (var item in flow.WithCancellation(ct).ConfigureAwait(false))
                    {
                        await writer.WriteAsync(item, ct).ConfigureAwait(false);
                    }
                });
            }
            return Task.CompletedTask;
        }, capacity);
    }

    public static IAsyncEnumerable<TResult> Combine<T1, T2, TResult>(
        IAsyncEnumerable<T1> flow1,
        IAsyncEnumerable<T2> flow2,
        Func<T1, T2, TResult> transform)
    {
        return ChannelFlow<TResult>((scope, writer) =>
        {
            var gate = new Lock();
            bool has1 = false, has2 = false;
            T1 latest1 = default!;
            T2 latest2 = default!;
            var signal = Channel.CreateBounded<TResult>(new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });

            void TryPublishLocked()
            {
                if (has1 && has2)
                {
                    signal.Writer.TryWrite(transform(latest1, latest2));
                }
            }

            var j1 = scope.Launch(async ct =>
            {
                await foreach (var v1 in flow1.WithCancellation(ct).ConfigureAwait(false))
                {
                    lock (gate)
                    {
                        latest1 = v1;
                        has1 = true;
                        TryPublishLocked();
                    }
                }
            });

            var j2 = scope.Launch(async ct =>
            {
                await foreach (var v2 in flow2.WithCancellation(ct).ConfigureAwait(false))
                {
                    lock (gate)
                    {
                        latest2 = v2;
                        has2 = true;
                        TryPublishLocked();
                    }
                }
            });

            scope.Launch(async _ =>
            {
                await Task.WhenAll(j1.JoinAsync(), j2.JoinAsync()).ConfigureAwait(false);
                signal.Writer.TryComplete();
            });

            scope.Launch(async ct =>
            {
                await foreach (var combined in signal.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                {
                    await writer.WriteAsync(combined, ct).ConfigureAwait(false);
                }
            });

            return Task.CompletedTask;
        }, capacity: 16);
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
        IAsyncEnumerable<T1> flow1,
        IAsyncEnumerable<T2> flow2,
        Func<T1, T2, TResult> transform,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var enum1 = flow1.GetAsyncEnumerator(cancellationToken);
        await using var enum2 = flow2.GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            var move1 = enum1.MoveNextAsync().AsTask();
            var move2 = enum2.MoveNextAsync().AsTask();
            await Task.WhenAll(move1, move2).ConfigureAwait(false);

            if (!move1.Result || !move2.Result)
                yield break;

            yield return transform(enum1.Current, enum2.Current);
        }
    }
}

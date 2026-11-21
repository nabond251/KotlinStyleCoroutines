using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace KotlinStyleCoroutines;

public static partial class Coroutine
{
    public static Suspendable<T> SuspendCancellable<T>(
        Action<Continuation<T>> block,
        CancellationToken token = default)
        => new(block, token);

    // Replicates Kotlin's `coroutineScope { ... }` suspending block
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
            // If the main scope body throws, cancel all background children before DisposeAsync awaits them
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

    // Replicates Kotlin's `supervisorScope { ... }`:
    // Child failures do not cancel sibling coroutines or fail the parent scope unless thrown directly from `block`.
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

    // Scoped WithContext overloads for TaskScheduler (e.g., Dispatchers.Default, LimitedParallelism, SingleThread)
    public static Task<T> WithContext<T>(
        TaskScheduler scheduler,
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Task.Factory.StartNew sets TaskScheduler.Current for the entire async execution tree inside `block`
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

    // Scoped WithContext overloads for SynchronizationContext (e.g., Dispatchers.Main)
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
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }, consumerCts.Token)
        .ContinueWith(t =>
        {
            if (t.IsFaulted)
                channel.Writer.TryComplete(t.Exception?.InnerException ?? t.Exception);
            else
                channel.Writer.TryComplete();
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

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
                _ = scope.Launch(async ct =>
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

    public static async IAsyncEnumerable<TResult> Combine<T1, T2, TResult>(
        IAsyncEnumerable<T1> flow1,
        IAsyncEnumerable<T2> flow2,
        Func<T1, T2, TResult> transform,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var gate = new Lock();
        bool has1 = false, has2 = false;
        T1 latest1 = default!;
        T2 latest2 = default!;
        var signal = Channel.CreateUnbounded<TResult>(new UnboundedChannelOptions
        {
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

        await using var scope = new CoroutineScope(cancellationToken, isSupervisor: false);
        try
        {
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

            _ = scope.Launch(async _ =>
            {
                await Task.WhenAll(j1.JoinAsync(), j2.JoinAsync()).ConfigureAwait(false);
                signal.Writer.TryComplete();
            });

            await foreach (var combined in signal.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return combined;
            }
        }
        finally
        {
            await scope.CancelAsync().ConfigureAwait(false);
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

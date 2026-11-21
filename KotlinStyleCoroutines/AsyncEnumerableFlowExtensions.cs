using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KotlinStyleCoroutines;

public static class AsyncEnumerableFlowExtensions
{
    public static Job LaunchIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        Func<T, CancellationToken, Task>? onEach = null)
    {
        return scope.Launch(async ct =>
        {
            await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
            {
                if (onEach is not null)
                {
                    await onEach(item, ct).ConfigureAwait(false);
                }
            }
        });
    }

    public static Job LaunchIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        Action<T> onEach)
    {
        return source.LaunchIn(scope, (item, _) =>
        {
            onEach(item);
            return Task.CompletedTask;
        });
    }

    public static async IAsyncEnumerable<T> FlowOn<T>(
        this IAsyncEnumerable<T> source,
        TaskScheduler scheduler,
        int bufferCapacity = 32,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(bufferCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var upstreamTask = Coroutine.WithContext(scheduler, async scope =>
        {
            try
            {
                await foreach (var item in source.WithCancellation(scope.Token))
                {
                    await channel.Writer.WriteAsync(item, scope.Token);
                }
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }, linkedCts.Token);

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(linkedCts.Token).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await linkedCts.CancelAsync().ConfigureAwait(false);
        }

        await upstreamTask.ConfigureAwait(false);
    }

    public static IAsyncEnumerable<TResult> CombineWith<T1, T2, TResult>(
        this IAsyncEnumerable<T1> first,
        IAsyncEnumerable<T2> second,
        Func<T1, T2, TResult> transform)
        => Coroutine.Combine(first, second, transform);

    public static IAsyncEnumerable<TResult> ZipWith<T1, T2, TResult>(
        this IAsyncEnumerable<T1> first,
        IAsyncEnumerable<T2> second,
        Func<T1, T2, TResult> transform)
        => Coroutine.Zip(first, second, transform);

    public static IAsyncEnumerable<TResult> TransformLatest<TSource, TResult>(
        this IAsyncEnumerable<TSource> source,
        Func<TSource, ChannelWriter<TResult>, CancellationToken, Task> transform)
    {
        return Coroutine.ChannelFlow<TResult>(async (scope, writer) =>
        {
            Job? activeJob = null;

            await foreach (var item in source.WithCancellation(scope.Token).ConfigureAwait(false))
            {
                if (activeJob is not null)
                {
                    activeJob.Cancel();
                    await activeJob.JoinAsync().ConfigureAwait(false);
                }

                activeJob = scope.Launch(ct => transform(item, writer, ct));
            }

            if (activeJob is not null)
            {
                await activeJob.JoinAsync().ConfigureAwait(false);
            }
        }, capacity: 16);
    }

    public static IAsyncEnumerable<TResult> FlatMapLatest<TSource, TResult>(
        this IAsyncEnumerable<TSource> source,
        Func<TSource, IAsyncEnumerable<TResult>> transform)
    {
        return source.TransformLatest<TSource, TResult>(async (item, writer, ct) =>
        {
            await foreach (var inner in transform(item).WithCancellation(ct).ConfigureAwait(false))
            {
                await writer.WriteAsync(inner, ct).ConfigureAwait(false);
            }
        });
    }

    public static IAsyncEnumerable<T> Debounce<T>(
        this IAsyncEnumerable<T> source,
        TimeSpan timeout,
        TimeProvider? timeProvider = null)
    {
        if (timeout <= TimeSpan.Zero)
            return source;

        var provider = timeProvider ?? TimeProvider.System;

        return Coroutine.ChannelFlow<T>(async (scope, writer) =>
        {
            var gate = new Lock();
            bool hasPending = false;
            T pendingValue = default!;
            long generation = 0;
            CancellationTokenSource? timerCts = null;

            async Task ScheduleEmitAsync(long expectedGen, CancellationToken scopeToken)
            {
                CancellationTokenSource localCts;
                lock (gate)
                {
                    timerCts?.Cancel();
                    timerCts?.Dispose();
                    timerCts = CancellationTokenSource.CreateLinkedTokenSource(scopeToken);
                    localCts = timerCts;
                }

                try
                {
                    await Task.Delay(timeout, provider, localCts.Token).ConfigureAwait(false);

                    T toEmit;
                    lock (gate)
                    {
                        if (!hasPending || generation != expectedGen)
                            return;

                        hasPending = false;
                        toEmit = pendingValue;
                        pendingValue = default!;
                    }

                    await writer.WriteAsync(toEmit, scopeToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Superseded by a newer upstream item or scope cancellation
                }
            }

            try
            {
                await foreach (var item in source.WithCancellation(scope.Token).ConfigureAwait(false))
                {
                    long nextGen;
                    lock (gate)
                    {
                        pendingValue = item;
                        hasPending = true;
                        nextGen = ++generation;
                    }

                    scope.Launch(ct => ScheduleEmitAsync(nextGen, ct));
                }

                T trailing = default!;
                bool emitTrailing = false;
                lock (gate)
                {
                    timerCts?.Cancel();
                    if (hasPending)
                    {
                        hasPending = false;
                        trailing = pendingValue;
                        pendingValue = default!;
                        emitTrailing = true;
                    }
                }

                if (emitTrailing)
                {
                    await writer.WriteAsync(trailing, scope.Token).ConfigureAwait(false);
                }
            }
            finally
            {
                lock (gate)
                {
                    timerCts?.Cancel();
                    timerCts?.Dispose();
                    timerCts = null;
                }
            }
        }, capacity: 16);
    }

    public static IAsyncEnumerable<T> Sample<T>(
        this IAsyncEnumerable<T> source,
        TimeSpan period,
        TimeProvider? timeProvider = null)
    {
        if (period <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(period), "Sample period must be positive.");

        var provider = timeProvider ?? TimeProvider.System;

        return Coroutine.ChannelFlow<T>(async (scope, writer) =>
        {
            var gate = new Lock();
            bool hasUnconsumed = false;
            T latest = default!;
            bool upstreamCompleted = false;

            var producerJob = scope.Launch(async ct =>
            {
                await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
                {
                    lock (gate)
                    {
                        latest = item;
                        hasUnconsumed = true;
                    }
                }

                lock (gate)
                {
                    upstreamCompleted = true;
                }
            });

            while (!scope.Token.IsCancellationRequested)
            {
                await Task.Delay(period, provider, scope.Token).ConfigureAwait(false);

                T toEmit = default!;
                bool shouldEmit = false;
                bool isDone;

                lock (gate)
                {
                    if (hasUnconsumed)
                    {
                        toEmit = latest;
                        latest = default!;
                        hasUnconsumed = false;
                        shouldEmit = true;
                    }
                    isDone = upstreamCompleted;
                }

                if (shouldEmit)
                {
                    await writer.WriteAsync(toEmit, scope.Token).ConfigureAwait(false);
                }

                if (isDone)
                    break;
            }

            await producerJob.JoinAsync().ConfigureAwait(false);
        }, capacity: 16);
    }

    public static async IAsyncEnumerable<T> Catch<T>(
        this IAsyncEnumerable<T> source,
        Func<Exception, ChannelWriter<T>, CancellationToken, Task> handler,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        Exception? upstreamError = null;
        await using var enumerator = source.GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            bool moved;
            try
            {
                moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                upstreamError = ex;
                break;
            }

            if (!moved)
                yield break;

            yield return enumerator.Current;
        }

        if (upstreamError is not null)
        {
            try
            {
                await handler(upstreamError, channel.Writer, cancellationToken).ConfigureAwait(false);
                channel.Writer.TryComplete();
            }
            catch (Exception handlerEx)
            {
                channel.Writer.TryComplete(handlerEx);
                throw;
            }

            await foreach (var fallback in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return fallback;
            }
        }
    }

    public static IAsyncEnumerable<T> Catch<T>(
        this IAsyncEnumerable<T> source,
        Func<Exception, T> fallbackSelector)
    {
        return source.Catch(async (ex, writer, ct) =>
        {
            await writer.WriteAsync(fallbackSelector(ex), ct).ConfigureAwait(false);
        });
    }

    public static async IAsyncEnumerable<T> RetryWhen<T>(
        this IAsyncEnumerable<T> source,
        Func<Exception, long, CancellationToken, ValueTask<bool>> predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Exception? failure = null;

            await using (var enumerator = source.GetAsyncEnumerator(cancellationToken))
            {
                while (true)
                {
                    bool moved;
                    try
                    {
                        moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        failure = ex;
                        break;
                    }

                    if (!moved)
                        yield break;

                    yield return enumerator.Current;
                }
            }

            bool shouldRetry = await predicate(failure!, attempt, cancellationToken).ConfigureAwait(false);
            if (!shouldRetry)
            {
                ExceptionDispatchInfo.Capture(failure!).Throw();
            }

            attempt++;
        }
    }

    public static IAsyncEnumerable<T> Retry<T>(
        this IAsyncEnumerable<T> source,
        long retries = long.MaxValue,
        Func<Exception, ValueTask<bool>>? predicate = null)
    {
        if (retries <= 0)
            throw new ArgumentOutOfRangeException(nameof(retries), "Retries must be greater than zero.");

        return source.RetryWhen(async (cause, attempt, _) =>
        {
            if (attempt >= retries)
                return false;

            return predicate is null || await predicate(cause).ConfigureAwait(false);
        });
    }

    public static IStateFlow<T> StateIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        ISharingStarted started,
        T initialValue,
        IEqualityComparer<T>? comparer = null)
    {
        var stateFlow = new MutableStateFlow<T>(initialValue, comparer);
        LaunchSharing(scope, source, stateFlow.SubscriptionCount, started,
            emitAsync: (value, _) =>
            {
                stateFlow.Value = value;
                return ValueTask.CompletedTask;
            },
            resetReplay: null);
        return stateFlow.AsStateFlow();
    }

    public static IStateFlow<T> StateIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        T initialValue,
        IEqualityComparer<T>? comparer = null)
        => source.StateIn(scope, SharingStarted.Eagerly, initialValue, comparer);

    public static ISharedFlow<T> ShareIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        ISharingStarted started,
        int replay = 0,
        int extraBufferCapacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
    {
        var sharedFlow = new MutableSharedFlow<T>(replay, extraBufferCapacity, onBufferOverflow);
        LaunchSharing(scope, source, sharedFlow.SubscriptionCount, started,
            emitAsync: (value, ct) => sharedFlow.EmitAsync(value, ct),
            resetReplay: sharedFlow.ResetReplayCache);
        return sharedFlow.AsSharedFlow();
    }

    public static ISharedFlow<T> ShareIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        int replay = 0,
        int extraBufferCapacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
        => source.ShareIn(scope, SharingStarted.Eagerly, replay, extraBufferCapacity, onBufferOverflow);

    private static void LaunchSharing<T>(
        CoroutineScope scope,
        IAsyncEnumerable<T> source,
        IStateFlow<int> subscriptionCount,
        ISharingStarted started,
        Func<T, CancellationToken, ValueTask> emitAsync,
        Action? resetReplay)
    {
        scope.Launch(async ct =>
        {
            Job? upstreamJob = null;

            await foreach (var command in started.Command(subscriptionCount).WithCancellation(ct).ConfigureAwait(false))
            {
                switch (command)
                {
                    case SharingCommand.Start:
                        if (upstreamJob is null || upstreamJob.IsCompleted)
                        {
                            upstreamJob = scope.Launch(async upstreamCt =>
                            {
                                await foreach (var item in source.WithCancellation(upstreamCt).ConfigureAwait(false))
                                {
                                    await emitAsync(item, upstreamCt).ConfigureAwait(false);
                                }
                            });
                        }
                        break;

                    case SharingCommand.Stop:
                        if (upstreamJob is not null)
                        {
                            upstreamJob.Cancel();
                            await upstreamJob.JoinAsync().ConfigureAwait(false);
                            upstreamJob = null;
                        }
                        break;

                    case SharingCommand.StopAndResetReplayCache:
                        if (upstreamJob is not null)
                        {
                            upstreamJob.Cancel();
                            await upstreamJob.JoinAsync().ConfigureAwait(false);
                            upstreamJob = null;
                        }
                        resetReplay?.Invoke();
                        break;
                }
            }
        });
    }
}

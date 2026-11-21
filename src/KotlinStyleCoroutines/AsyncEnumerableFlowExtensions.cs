using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace KotlinStyleCoroutines;

public static class AsyncEnumerableFlowExtensions
{
    public static Job LaunchIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        Func<T, CancellationToken, Task> onEach)
    {
        return scope.Launch(async ct =>
        {
            await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
            {
                await onEach(item, ct).ConfigureAwait(false);
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
        int bufferCapacity = 64,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var effectiveParent = Coroutine.ResolveToken(cancellationToken);
        var channel = Channel.CreateBounded<T>(bufferCapacity);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveParent);

        var upstreamTask = Coroutine.WithContext(scheduler, async scope =>
        {
            try
            {
                await foreach (var item in source.WithCancellation(scope.Token).ConfigureAwait(false))
                {
                    await channel.Writer.WriteAsync(item, scope.Token).ConfigureAwait(false);
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
            await foreach (var item in channel.Reader.ReadAllAsync(effectiveParent).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await linkedCts.CancelAsync().ConfigureAwait(false);
            try
            {
                await upstreamTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (linkedCts.IsCancellationRequested)
            {
            }
        }
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

    // ========================================================================
    // Latest-Switching Operators (TransformLatest & FlatMapLatest)
    // ========================================================================

    // Replicates Kotlin's `flow.transformLatest { value -> emit(...) }`:
    // Cancels the previous in-flight transformation job whenever a new upstream value arrives.
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
                    activeJob.Dispose();
                }

                var current = item;
                activeJob = scope.Launch(ct => transform(current, writer, ct));
            }

            if (activeJob is not null)
            {
                await activeJob.JoinAsync().ConfigureAwait(false);
                activeJob.Dispose();
            }
        }, capacity: 16);
    }

    // Replicates Kotlin's `flow.flatMapLatest { value -> subFlow }`:
    // Switches to collecting the latest inner `IAsyncEnumerable<TResult>` and cancels the previous one.
    public static IAsyncEnumerable<TResult> FlatMapLatest<TSource, TResult>(
        this IAsyncEnumerable<TSource> source,
        Func<TSource, IAsyncEnumerable<TResult>> transform)
    {
        return source.TransformLatest<TSource, TResult>(async (item, writer, ct) =>
        {
            await foreach (var innerItem in transform(item).WithCancellation(ct).ConfigureAwait(false))
            {
                await writer.WriteAsync(innerItem, ct).ConfigureAwait(false);
            }
        });
    }

    // ========================================================================
    // Time-Based Flow Operators (Debounce & Sample)
    // ========================================================================

    public static IAsyncEnumerable<T> Debounce<T>(
        this IAsyncEnumerable<T> source,
        TimeSpan timeout,
        TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        var clock = timeProvider ?? TimeProvider.System;

        return Coroutine.ChannelFlow<T>(async (scope, writer) =>
        {
            var sync = new Lock();
            T? latestValue = default;
            var hasPending = false;
            var generation = 0L;
            Job? timerJob = null;

            async Task ScheduleEmitAsync(long expectedGen, CancellationToken ct)
            {
                try
                {
                    await Task.Delay(timeout, clock, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                T valueToEmit;
                lock (sync)
                {
                    if (!hasPending || generation != expectedGen)
                        return;

                    valueToEmit = latestValue!;
                    hasPending = false;
                }

                await writer.WriteAsync(valueToEmit, ct).ConfigureAwait(false);
            }

            await foreach (var item in source.WithCancellation(scope.Token).ConfigureAwait(false))
            {
                long nextGen;
                Job? previousTimer;
                lock (sync)
                {
                    latestValue = item;
                    hasPending = true;
                    nextGen = ++generation;
                    previousTimer = timerJob;
                }

                previousTimer?.Cancel();
                timerJob = scope.Launch(ct => ScheduleEmitAsync(nextGen, ct));
            }

            timerJob?.Cancel();

            T? trailing = default;
            var emitTrailing = false;
            lock (sync)
            {
                if (hasPending)
                {
                    trailing = latestValue;
                    hasPending = false;
                    emitTrailing = true;
                }
            }

            if (emitTrailing)
            {
                await writer.WriteAsync(trailing!, scope.Token).ConfigureAwait(false);
            }
        }, capacity: 16);
    }

    public static IAsyncEnumerable<T> Sample<T>(
    this IAsyncEnumerable<T> source,
    TimeSpan period,
    TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (period <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(period), "Sample period must be positive.");

        var tp = timeProvider ?? TimeProvider.System;

        return Coroutine.ChannelFlow<T>(async (scope, writer) =>
        {
            var gate = new Lock();
            T? latest = default;
            var hasValue = false;

            var tickerJob = scope.Launch(async innerCt =>
            {
                while (!innerCt.IsCancellationRequested)
                {
                    await Task.Delay(period, tp, innerCt).ConfigureAwait(false);

                    T? toEmit = default;
                    var shouldEmit = false;

                    lock (gate)
                    {
                        if (hasValue)
                        {
                            toEmit = latest;
                            hasValue = false;
                            shouldEmit = true;
                        }
                    }

                    if (shouldEmit)
                    {
                        await writer.WriteAsync(toEmit!, innerCt).ConfigureAwait(false);
                    }
                }
            });

            await foreach (var item in source.WithCancellation(scope.Token).ConfigureAwait(false))
            {
                lock (gate)
                {
                    latest = item;
                    hasValue = true;
                }
            }

            tickerJob.Cancel();
            await tickerJob.JoinAsync().ConfigureAwait(false);
        }, capacity: 16);
    }

    // ========================================================================
    // Exception Recovery Operators (Catch & RetryWhen)
    // ========================================================================

    public static async IAsyncEnumerable<T> Catch<T>(
        this IAsyncEnumerable<T> source,
        Func<Exception, ChannelWriter<T>, CancellationToken, Task> handler,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var effectiveParent = Coroutine.ResolveToken(cancellationToken);
        await using var enumerator = source.GetAsyncEnumerator(effectiveParent);
        ExceptionDispatchInfo? capturedError = null;

        while (true)
        {
            bool hasNext;
            try
            {
                hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (effectiveParent.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                capturedError = ExceptionDispatchInfo.Capture(ex);
                break;
            }

            if (!hasNext)
                yield break;

            yield return enumerator.Current;
        }

        if (capturedError is not null)
        {
            var fallbackChannel = Channel.CreateUnbounded<T>();
            var handlerTask = RunCatchHandlerAsync(handler, capturedError.SourceException, fallbackChannel.Writer, effectiveParent);

            await foreach (var fallbackItem in fallbackChannel.Reader.ReadAllAsync(effectiveParent).ConfigureAwait(false))
            {
                yield return fallbackItem;
            }

            await handlerTask.ConfigureAwait(false);
        }
    }

    private static async Task RunCatchHandlerAsync<T>(
        Func<Exception, ChannelWriter<T>, CancellationToken, Task> handler,
        Exception error,
        ChannelWriter<T> writer,
        CancellationToken ct)
    {
        try
        {
            await handler(error, writer, ct).ConfigureAwait(false);
            writer.TryComplete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
            throw;
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
        var effectiveParent = Coroutine.ResolveToken(cancellationToken);
        long attempt = 0;

        while (true)
        {
            effectiveParent.ThrowIfCancellationRequested();
            ExceptionDispatchInfo? attemptError = null;

            await using (var enumerator = source.GetAsyncEnumerator(effectiveParent))
            {
                while (true)
                {
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (effectiveParent.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        attemptError = ExceptionDispatchInfo.Capture(ex);
                        break;
                    }

                    if (!hasNext)
                        yield break;

                    yield return enumerator.Current;
                }
            }

            if (attemptError is not null)
            {
                var shouldRetry = await predicate(attemptError.SourceException, attempt, effectiveParent).ConfigureAwait(false);
                if (!shouldRetry)
                {
                    attemptError.Throw();
                }

                attempt++;
            }
        }
    }

    public static IAsyncEnumerable<T> Retry<T>(
        this IAsyncEnumerable<T> source,
        long retries = long.MaxValue,
        Func<Exception, ValueTask<bool>>? predicate = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retries);

        return source.RetryWhen(async (ex, attempt, _) =>
        {
            if (attempt >= retries)
                return false;

            return predicate is null || await predicate(ex).ConfigureAwait(false);
        });
    }

    // ========================================================================
    // Hot Stream Conversion Extensions (StateIn & ShareIn)
    // ========================================================================

    public static IStateFlow<T> StateIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        ISharingStarted started,
        T initialValue)
    {
        var stateFlow = new MutableStateFlow<T>(initialValue);
        LaunchSharing(source, scope, started, stateFlow.SubscriptionCount,
            onItem: item =>
            {
                stateFlow.Value = item;
                return ValueTask.CompletedTask;
            },
            onResetReplay: () => { });

        return stateFlow;
    }

    public static IStateFlow<T> StateIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        T initialValue)
        => source.StateIn(scope, SharingStarted.Eagerly, initialValue);

    public static ISharedFlow<T> ShareIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        ISharingStarted started,
        int replay = 0,
        int extraBufferCapacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
    {
        var sharedFlow = new MutableSharedFlow<T>(replay, extraBufferCapacity, onBufferOverflow);
        LaunchSharing(source, scope, started, sharedFlow.SubscriptionCount,
            onItem: item => sharedFlow.EmitAsync(item, scope.Token),
            onResetReplay: sharedFlow.ResetReplayCache);

        return sharedFlow;
    }

    public static ISharedFlow<T> ShareIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        int replay = 0,
        int extraBufferCapacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
        => source.ShareIn(scope, SharingStarted.Eagerly, replay, extraBufferCapacity, onBufferOverflow);

    private static void LaunchSharing<T>(
        IAsyncEnumerable<T> source,
        CoroutineScope scope,
        ISharingStarted started,
        IStateFlow<int> subscriptionCount,
        Func<T, ValueTask> onItem,
        Action onResetReplay)
    {
        _ = scope.Launch(async ct =>
        {
            Job? upstreamJob = null;
            var isRunning = false;

            try
            {
                await foreach (var command in started.Command(subscriptionCount).WithCancellation(ct).ConfigureAwait(false))
                {
                    switch (command)
                    {
                        case SharingCommand.Start when !isRunning:
                            isRunning = true;
                            upstreamJob = scope.Launch(async upstreamCt =>
                            {
                                await foreach (var item in source.WithCancellation(upstreamCt).ConfigureAwait(false))
                                {
                                    await onItem(item).ConfigureAwait(false);
                                }
                            });
                            break;

                        case SharingCommand.Stop when isRunning:
                            isRunning = false;
                            if (upstreamJob is not null)
                            {
                                upstreamJob.Cancel();
                                await upstreamJob.JoinAsync().ConfigureAwait(false);
                                upstreamJob.Dispose();
                                upstreamJob = null;
                            }
                            break;

                        case SharingCommand.StopAndResetReplayCache:
                            isRunning = false;
                            if (upstreamJob is not null)
                            {
                                upstreamJob.Cancel();
                                await upstreamJob.JoinAsync().ConfigureAwait(false);
                                upstreamJob.Dispose();
                                upstreamJob = null;
                            }
                            onResetReplay();
                            break;
                    }
                }
            }
            finally
            {
                if (upstreamJob is not null)
                {
                    upstreamJob.Cancel();
                    await upstreamJob.JoinAsync().ConfigureAwait(false);
                    upstreamJob.Dispose();
                }
            }
        });
    }
}

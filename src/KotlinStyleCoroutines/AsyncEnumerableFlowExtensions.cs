using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace KotlinStyleCoroutines;

public static class AsyncEnumerableFlowExtensions
{
    // ========================================================================
    // Terminal Collection & Context Operators (LaunchIn & FlowOn)
    // ========================================================================

    public static Job LaunchIn<T>(
        this IAsyncEnumerable<T> flow,
        CoroutineScope scope,
        Func<T, CancellationToken, Task> onEach)
    {
        return scope.Launch(async ct =>
        {
            await foreach (var item in flow.WithCancellation(ct).ConfigureAwait(false))
            {
                await onEach(item, ct).ConfigureAwait(false);
            }
        });
    }

    public static Job LaunchIn<T>(
        this IAsyncEnumerable<T> flow,
        CoroutineScope scope,
        Action<T> onEach)
    {
        return flow.LaunchIn(scope, (item, _) =>
        {
            onEach(item);
            return Task.CompletedTask;
        });
    }

    public static async IAsyncEnumerable<T> FlowOn<T>(
        this IAsyncEnumerable<T> flow,
        TaskScheduler scheduler,
        int bufferCapacity = 64,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var effectiveToken = Coroutine.ResolveToken(cancellationToken);
        var channel = Channel.CreateBounded<T>(bufferCapacity);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveToken);

        var upstreamTask = Coroutine.WithContext(scheduler, async scope =>
        {
            try
            {
                await foreach (var item in flow.WithCancellation(scope.Token).ConfigureAwait(false))
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
            await foreach (var item in channel.Reader.ReadAllAsync(effectiveToken).ConfigureAwait(false))
            {
                yield return item;
            }

            await upstreamTask.ConfigureAwait(false);
        }
        finally
        {
            await linkedCts.CancelAsync().ConfigureAwait(false);
        }
    }

    // ========================================================================
    // Fluent Flow Combinators (CombineWith & ZipWith)
    // ========================================================================

    public static IAsyncEnumerable<TResult> CombineWith<T1, T2, TResult>(
        this IAsyncEnumerable<T1> first,
        IAsyncEnumerable<T2> second,
        Func<T1, T2, TResult> transform)
        => Coroutine.Combine(first, second, transform);

    public static IAsyncEnumerable<TResult> CombineWith<T1, T2, T3, TResult>(
        this IAsyncEnumerable<T1> first,
        IAsyncEnumerable<T2> second,
        IAsyncEnumerable<T3> third,
        Func<T1, T2, T3, TResult> transform)
        => Coroutine.Combine(first, second, third, transform);

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
                }

                var captured = item;
                activeJob = scope.Launch(ct => transform(captured, writer, ct));
            }

            if (activeJob is not null)
            {
                await activeJob.JoinAsync().ConfigureAwait(false);
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
        var provider = timeProvider ?? TimeProvider.System;

        return Coroutine.ChannelFlow<T>(async (scope, writer) =>
        {
            var gate = new Lock();
            long generation = 0;
            bool hasPending = false;
            T pendingValue = default!;
            Job? timerJob = null;

            async Task ScheduleEmitAsync(long expectedGen, CancellationToken ct)
            {
                await Task.Delay(timeout, provider, ct).ConfigureAwait(false);

                T valueToEmit = default!;
                bool shouldEmit = false;
                lock (gate)
                {
                    if (hasPending && generation == expectedGen)
                    {
                        valueToEmit = pendingValue;
                        hasPending = false;
                        shouldEmit = true;
                    }
                }

                if (shouldEmit)
                {
                    await writer.WriteAsync(valueToEmit, ct).ConfigureAwait(false);
                }
            }

            await foreach (var item in source.WithCancellation(scope.Token).ConfigureAwait(false))
            {
                long nextGen;
                lock (gate)
                {
                    pendingValue = item;
                    hasPending = true;
                    nextGen = ++generation;
                }

                timerJob?.Cancel();
                timerJob = scope.Launch(ct => ScheduleEmitAsync(nextGen, ct));
            }

            // Flush any trailing debounced value upon upstream completion
            timerJob?.Cancel();

            T trailingValue = default!;
            bool flushTrailing = false;
            lock (gate)
            {
                if (hasPending)
                {
                    trailingValue = pendingValue;
                    hasPending = false;
                    flushTrailing = true;
                }
            }

            if (flushTrailing)
            {
                await writer.WriteAsync(trailingValue, scope.Token).ConfigureAwait(false);
            }
        }, capacity: 16);
    }

    public static IAsyncEnumerable<T> Sample<T>(
        this IAsyncEnumerable<T> source,
        TimeSpan period,
        TimeProvider? timeProvider = null)
    {
        var provider = timeProvider ?? TimeProvider.System;

        return Coroutine.ChannelFlow<T>(async (scope, writer) =>
        {
            var gate = new Lock();
            bool hasValue = false;
            T latest = default!;
            bool completed = false;

            var tickerJob = scope.Launch(async ct =>
            {
                while (!ct.IsCancellationRequested && !Volatile.Read(ref completed))
                {
                    await Task.Delay(period, provider, ct).ConfigureAwait(false);

                    T valueToEmit = default!;
                    bool shouldEmit = false;
                    lock (gate)
                    {
                        if (hasValue)
                        {
                            valueToEmit = latest;
                            hasValue = false;
                            shouldEmit = true;
                        }
                    }

                    if (shouldEmit)
                    {
                        await writer.WriteAsync(valueToEmit, ct).ConfigureAwait(false);
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

            Volatile.Write(ref completed, true);
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
        var effectiveToken = Coroutine.ResolveToken(cancellationToken);
        await using var enumerator = source.GetAsyncEnumerator(effectiveToken);

        Exception? caught = null;
        while (true)
        {
            bool hasNext;
            T current = default!;
            try
            {
                hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                if (hasNext)
                {
                    current = enumerator.Current;
                }
            }
            catch (OperationCanceledException) when (effectiveToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                caught = ex;
                break;
            }

            if (!hasNext)
            {
                yield break;
            }

            yield return current;
        }

        if (caught is not null)
        {
            var fallbackChannel = Channel.CreateUnbounded<T>();
            var handlerTask = Task.Run(async () =>
            {
                try
                {
                    await handler(caught, fallbackChannel.Writer, effectiveToken).ConfigureAwait(false);
                    fallbackChannel.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    fallbackChannel.Writer.TryComplete(ex);
                }
            }, effectiveToken);

            await foreach (var fallbackItem in fallbackChannel.Reader.ReadAllAsync(effectiveToken).ConfigureAwait(false))
            {
                yield return fallbackItem;
            }

            await handlerTask.ConfigureAwait(false);
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
        var effectiveToken = Coroutine.ResolveToken(cancellationToken);
        long attempt = 0;

        while (true)
        {
            effectiveToken.ThrowIfCancellationRequested();
            await using var enumerator = source.GetAsyncEnumerator(effectiveToken);
            Exception? failure = null;

            while (true)
            {
                bool hasNext;
                T current = default!;
                try
                {
                    hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    if (hasNext)
                    {
                        current = enumerator.Current;
                    }
                }
                catch (OperationCanceledException) when (effectiveToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    failure = ex;
                    break;
                }

                if (!hasNext)
                {
                    yield break;
                }

                yield return current;
            }

            if (failure is not null)
            {
                bool shouldRetry = await predicate(failure, attempt++, effectiveToken).ConfigureAwait(false);
                if (!shouldRetry)
                {
                    ExceptionDispatchInfo.Capture(failure).Throw();
                }
            }
        }
    }

    public static IAsyncEnumerable<T> Retry<T>(
        this IAsyncEnumerable<T> source,
        long retries = long.MaxValue,
        Func<Exception, bool>? predicate = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(retries);

        return source.RetryWhen((ex, attempt, _) =>
        {
            bool allowed = attempt < retries && (predicate?.Invoke(ex) ?? true);
            return ValueTask.FromResult(allowed);
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
        LaunchSharing(source, scope, started, stateFlow.SubscriptionCount, stateFlow.EmitAsync, onResetReplay: null);
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
        int replay = 0)
    {
        var sharedFlow = new MutableSharedFlow<T>(replay: replay);
        LaunchSharing(
            source,
            scope,
            started,
            sharedFlow.SubscriptionCount,
            sharedFlow.EmitAsync,
            sharedFlow.ResetReplayCache);
        return sharedFlow;
    }

    public static ISharedFlow<T> ShareIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        int replay = 0)
        => source.ShareIn(scope, SharingStarted.Eagerly, replay);

    private static void LaunchSharing<T>(
        IAsyncEnumerable<T> source,
        CoroutineScope scope,
        ISharingStarted started,
        IStateFlow<int> subscriptionCount,
        Func<T, CancellationToken, Task> emitAsync,
        Action? onResetReplay)
    {
        _ = scope.Launch(async ct =>
        {
            Job? upstreamJob = null;

            await foreach (var command in started.Command(subscriptionCount).WithCancellation(ct).ConfigureAwait(false))
            {
                switch (command)
                {
                    case SharingCommand.Start:
                        if (upstreamJob is not { IsActive: true })
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
                        onResetReplay?.Invoke();
                        break;
                }
            }
        });
    }
}

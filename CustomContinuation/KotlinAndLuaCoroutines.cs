using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KotlinStyleCoroutines;

// ============================================================================
// PART 1: Kotlin-style Single-Shot Custom Continuation (suspendCancellableCoroutine)
// ============================================================================

public sealed class Continuation<T> : INotifyCompletion
{
    private Action? _continuation;
    private T? _result;
    private ExceptionDispatchInfo? _exception;
    private int _state; // 0 = Pending, 1 = Completed, 2 = Canceled
    private Action? _onCancel;

    public bool IsCompleted => Volatile.Read(ref _state) != 0;

    public void OnCompleted(Action continuation)
    {
        if (Interlocked.CompareExchange(ref _continuation, continuation, null) != null)
        {
            continuation();
        }
    }

    public T GetResult()
    {
        if (_state == 2)
            throw new OperationCanceledException();

        _exception?.Throw();
        return _result!;
    }

    public void Resume(T value)
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            return;

        _result = value;
        Complete();
    }

    public void ResumeWithException(Exception exception)
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            return;

        _exception = ExceptionDispatchInfo.Capture(exception);
        Complete();
    }

    public void InvokeOnCancellation(Action onCancel)
    {
        _onCancel = onCancel;
        if (Volatile.Read(ref _state) == 2)
            onCancel();
    }

    internal void Cancel()
    {
        if (Interlocked.CompareExchange(ref _state, 2, 0) != 0)
            return;

        _onCancel?.Invoke();
        Complete();
    }

    private void Complete()
    {
        var cont = Interlocked.Exchange(ref _continuation, static () => { });
        cont?.Invoke();
    }
}

public readonly struct Suspendable<T>
{
    private readonly Continuation<T> _continuation;

    public Suspendable(Action<Continuation<T>> block, CancellationToken token = default)
    {
        _continuation = new Continuation<T>();

        if (token.CanBeCanceled)
        {
            token.Register(static state => ((Continuation<T>)state!).Cancel(), _continuation);
        }

        block(_continuation);
    }

    public Continuation<T> GetAwaiter() => _continuation;
}

// ============================================================================
// PART 2: Kotlin-style Job, Deferred<T>, Dispatchers, & Structured Concurrency
// ============================================================================

public class Job : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly Task _task;

    internal Job(CancellationTokenSource cts, Task task)
    {
        _cts = cts;
        _task = task;
    }

    public CancellationToken Token => _cts.Token;
    public bool IsActive => !_task.IsCompleted && !_cts.IsCancellationRequested;
    public bool IsCompleted => _task.IsCompleted;
    public bool IsCanceled => _cts.IsCancellationRequested || _task.IsCanceled;

    public void Cancel()
    {
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Job has already completed and disposed its CTS
        }
    }

    public Task JoinAsync() => _task;

    public TaskAwaiter GetAwaiter() => _task.GetAwaiter();

    public void Dispose() => _cts.Dispose();
}

public sealed class Deferred<T> : Job
{
    private readonly Task<T> _typedTask;

    internal Deferred(CancellationTokenSource cts, Task<T> task)
        : base(cts, task)
    {
        _typedTask = task;
    }

    public Task<T> AwaitAsync() => _typedTask;

    public new TaskAwaiter<T> GetAwaiter() => _typedTask.GetAwaiter();
}

public static class Dispatchers
{
    public static TaskScheduler Default => TaskScheduler.Default;

    public static SynchronizationContext? Main { get; set; }

    public static TaskScheduler LimitedParallelism(int maxDegreeOfParallelism)
        => new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, maxDegreeOfParallelism).ConcurrentScheduler;

    public static TaskScheduler SingleThread()
        => new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, maxConcurrencyLevel: 1).ExclusiveScheduler;
}

public sealed class CoroutineScope : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly ConcurrentBag<Task> _children = new();
    private readonly bool _isSupervisor;
    private readonly Action<Exception>? _onChildException;
    private ExceptionDispatchInfo? _firstException;

    public CancellationToken Token => _cts.Token;

    internal CoroutineScope(
        CancellationToken parentToken,
        bool isSupervisor = false,
        Action<Exception>? onChildException = null)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(parentToken);
        _isSupervisor = isSupervisor;
        _onChildException = onChildException;
    }

    public Job Launch(Func<CancellationToken, Task> block)
    {
        var childCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var task = RunChildAsync(block, childCts);
        _children.Add(task);
        return new Job(childCts, task);
    }

    public Deferred<T> Async<T>(Func<CancellationToken, Task<T>> block)
    {
        var childCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var task = RunChildAsync(block, childCts);
        _children.Add(task);
        return new Deferred<T>(childCts, task);
    }

    public void Cancel() => _cts.Cancel();

    internal Task CancelAsync() => _cts.CancelAsync();

    private async Task RunChildAsync(Func<CancellationToken, Task> block, CancellationTokenSource childCts)
    {
        try
        {
            await block(childCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (childCts.IsCancellationRequested)
        {
            // Expected cancellation of this specific Job or its parent scope
        }
        catch (Exception ex)
        {
            if (_isSupervisor)
            {
                _onChildException?.Invoke(ex);
                return;
            }

            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
        }
        finally
        {
            childCts.Dispose();
        }
    }

    private async Task<T> RunChildAsync<T>(Func<CancellationToken, Task<T>> block, CancellationTokenSource childCts)
    {
        try
        {
            return await block(childCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (childCts.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (_isSupervisor)
            {
                _onChildException?.Invoke(ex);
                throw;
            }

            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            childCts.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        while (_children.TryTake(out var child))
        {
            try
            {
                await child.ConfigureAwait(false);
            }
            catch
            {
                // Exceptions are already captured in _firstException, reported to _onChildException, or awaited by callers
            }
        }

        _cts.Dispose();
        _firstException?.Throw();
    }
}

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

public enum BufferOverflow
{
    Suspend,
    DropOldest,
    DropLatest
}

public enum SharingCommand
{
    Start,
    Stop,
    StopAndResetReplayCache
}

public interface ISharingStarted
{
    IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount);
}

public static class SharingStarted
{
    public static ISharingStarted Eagerly { get; } = new EagerlySharingStarted();
    public static ISharingStarted Lazily { get; } = new LazilySharingStarted();

    public static ISharingStarted WhileSubscribed(
        TimeSpan? stopTimeout = null,
        TimeSpan? replayExpiration = null)
        => new WhileSubscribedSharingStarted(
            stopTimeout ?? TimeSpan.Zero,
            replayExpiration);

    private sealed class EagerlySharingStarted : ISharingStarted
    {
        public async IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount)
        {
            yield return SharingCommand.Start;
            await Task.CompletedTask;
        }
    }

    private sealed class LazilySharingStarted : ISharingStarted
    {
        public async IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount)
        {
            await foreach (int count in subscriptionCount.ConfigureAwait(false))
            {
                if (count > 0)
                {
                    yield return SharingCommand.Start;
                    yield break;
                }
            }
        }
    }

    private sealed class WhileSubscribedSharingStarted : ISharingStarted
    {
        private readonly TimeSpan _stopTimeout;
        private readonly TimeSpan? _replayExpiration;

        public WhileSubscribedSharingStarted(TimeSpan stopTimeout, TimeSpan? replayExpiration)
        {
            _stopTimeout = stopTimeout;
            _replayExpiration = replayExpiration;
        }

        public async IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount)
        {
            using var enumerator = subscriptionCount.GetAsyncEnumerator();
            bool isRunning = false;
            CancellationTokenSource? stopCts = null;

            try
            {
                while (await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    int count = enumerator.Current;

                    if (count > 0)
                    {
                        if (stopCts is not null)
                        {
                            await stopCts.CancelAsync().ConfigureAwait(false);
                            stopCts.Dispose();
                            stopCts = null;
                        }

                        if (!isRunning)
                        {
                            isRunning = true;
                            yield return SharingCommand.Start;
                        }
                    }
                    else if (isRunning && stopCts is null)
                    {
                        if (_stopTimeout <= TimeSpan.Zero)
                        {
                            isRunning = false;
                            yield return SharingCommand.Stop;

                            if (_replayExpiration is { } exp && exp <= TimeSpan.Zero)
                            {
                                yield return SharingCommand.StopAndResetReplayCache;
                            }
                        }
                        else
                        {
                            stopCts = new CancellationTokenSource();
                            try
                            {
                                await Task.Delay(_stopTimeout, stopCts.Token).ConfigureAwait(false);
                                isRunning = false;
                                stopCts.Dispose();
                                stopCts = null;
                                yield return SharingCommand.Stop;

                                if (_replayExpiration is { } exp)
                                {
                                    if (exp > TimeSpan.Zero)
                                        await Task.Delay(exp).ConfigureAwait(false);
                                    if (subscriptionCount.Value == 0)
                                        yield return SharingCommand.StopAndResetReplayCache;
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                // Resubscribed before stopTimeout elapsed
                            }
                        }
                    }
                }
            }
            finally
            {
                stopCts?.Dispose();
            }
        }
    }
}

public interface ISharedFlow<out T> : IAsyncEnumerable<T>
{
    IReadOnlyList<T> ReplayCache { get; }
    IStateFlow<int> SubscriptionCount { get; }
}

public sealed class MutableSharedFlow<T> : ISharedFlow<T>
{
    private readonly Lock _gate = new();
    private readonly int _replay;
    private readonly int _bufferCapacity;
    private readonly BufferOverflow _onBufferOverflow;
    private readonly Queue<T> _replayBuffer = new();
    private readonly List<Subscriber> _subscribers = new();
    private readonly MutableStateFlow<int> _subscriptionCount = new(0);

    private sealed class Subscriber
    {
        public readonly Channel<T> Channel;

        public Subscriber(int capacity, BufferOverflow overflow)
        {
            int effectiveCapacity = capacity <= 0 ? 1 : capacity;
            Channel = System.Threading.Channels.Channel.CreateBounded<T>(
                new BoundedChannelOptions(effectiveCapacity)
                {
                    FullMode = overflow switch
                    {
                        BufferOverflow.DropOldest => BoundedChannelFullMode.DropOldest,
                        BufferOverflow.DropLatest => BoundedChannelFullMode.DropWrite,
                        _ => BoundedChannelFullMode.Wait
                    },
                    SingleReader = true,
                    SingleWriter = false
                });
        }
    }

    public MutableSharedFlow(
        int replay = 0,
        int extraBufferCapacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
    {
        if (replay < 0)
            throw new ArgumentOutOfRangeException(nameof(replay), "Replay must be non-negative.");
        if (extraBufferCapacity < 0)
            throw new ArgumentOutOfRangeException(nameof(extraBufferCapacity), "ExtraBufferCapacity must be non-negative.");

        _replay = replay;
        _bufferCapacity = replay + extraBufferCapacity;
        _onBufferOverflow = onBufferOverflow;
    }

    public IReadOnlyList<T> ReplayCache
    {
        get
        {
            lock (_gate)
            {
                return _replayBuffer.ToArray();
            }
        }
    }

    public IStateFlow<int> SubscriptionCount => _subscriptionCount;

    public ISharedFlow<T> AsSharedFlow() => this;

    public void ResetReplayCache()
    {
        lock (_gate)
        {
            _replayBuffer.Clear();
        }
    }

    public bool TryEmit(T value)
    {
        Subscriber[] snapshot;
        lock (_gate)
        {
            RecordReplayLocked(value);
            snapshot = _subscribers.ToArray();
        }

        bool allAccepted = true;
        foreach (var sub in snapshot)
        {
            if (!sub.Channel.Writer.TryWrite(value))
            {
                allAccepted = false;
            }
        }

        return allAccepted;
    }

    public async ValueTask EmitAsync(T value, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Subscriber[] snapshot;
        lock (_gate)
        {
            RecordReplayLocked(value);
            snapshot = _subscribers.ToArray();
        }

        foreach (var sub in snapshot)
        {
            if (_onBufferOverflow == BufferOverflow.Suspend)
            {
                await sub.Channel.Writer.WriteAsync(value, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                sub.Channel.Writer.TryWrite(value);
            }
        }
    }

    private void RecordReplayLocked(T value)
    {
        if (_replay <= 0)
            return;

        if (_replayBuffer.Count == _replay)
            _replayBuffer.Dequeue();

        _replayBuffer.Enqueue(value);
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var subscriber = new Subscriber(_bufferCapacity, _onBufferOverflow);
        T[] initialReplay;
        int newCount;

        lock (_gate)
        {
            initialReplay = _replayBuffer.ToArray();
            _subscribers.Add(subscriber);
            newCount = _subscribers.Count;
        }
        _subscriptionCount.Value = newCount;

        try
        {
            foreach (var cached in initialReplay)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return cached;
            }

            await foreach (var item in subscriber.Channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            int remaining;
            lock (_gate)
            {
                _subscribers.Remove(subscriber);
                remaining = _subscribers.Count;
            }
            _subscriptionCount.Value = remaining;
            subscriber.Channel.Writer.TryComplete();
        }
    }
}

public interface IStateFlow<out T> : ISharedFlow<T>
{
    T Value { get; }
}

public sealed class MutableStateFlow<T> : IStateFlow<T>
{
    private readonly Lock _gate = new();
    private readonly IEqualityComparer<T> _comparer;
    private readonly List<Subscriber> _subscribers = new();
    private MutableStateFlow<int>? _subscriptionCount;
    private T _value;
    private long _version;

    private sealed class Subscriber
    {
        public readonly Channel<T> Channel = System.Threading.Channels.Channel.CreateBounded<T>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });
    }

    public MutableStateFlow(T initialValue, IEqualityComparer<T>? comparer = null)
    {
        _value = initialValue;
        _comparer = comparer ?? EqualityComparer<T>.Default;
    }

    public T Value
    {
        get
        {
            lock (_gate)
            {
                return _value;
            }
        }
        set => TryEmit(value);
    }

    public IReadOnlyList<T> ReplayCache
    {
        get
        {
            lock (_gate)
            {
                return [_value];
            }
        }
    }

    public IStateFlow<int> SubscriptionCount
    {
        get
        {
            lock (_gate)
            {
                return _subscriptionCount ??= new MutableStateFlow<int>(_subscribers.Count);
            }
        }
    }

    public IStateFlow<T> AsStateFlow() => this;

    public bool TryEmit(T newValue)
    {
        Subscriber[] snapshot;
        lock (_gate)
        {
            if (_comparer.Equals(_value, newValue))
                return false;

            _value = newValue;
            _version++;
            snapshot = _subscribers.ToArray();
        }

        foreach (var sub in snapshot)
        {
            sub.Channel.Writer.TryWrite(newValue);
        }

        return true;
    }

    public T UpdateAndGet(Func<T, T> transform)
    {
        while (true)
        {
            T prev;
            lock (_gate)
            {
                prev = _value;
            }

            T next = transform(prev);
            if (CompareAndSet(prev, next))
                return next;
        }
    }

    public T GetAndUpdate(Func<T, T> transform)
    {
        while (true)
        {
            T prev;
            lock (_gate)
            {
                prev = _value;
            }

            T next = transform(prev);
            if (CompareAndSet(prev, next))
                return prev;
        }
    }

    public bool CompareAndSet(T expect, T update)
    {
        Subscriber[] snapshot;
        lock (_gate)
        {
            if (!_comparer.Equals(_value, expect))
                return false;

            if (_comparer.Equals(_value, update))
                return true;

            _value = update;
            _version++;
            snapshot = _subscribers.ToArray();
        }

        foreach (var sub in snapshot)
        {
            sub.Channel.Writer.TryWrite(update);
        }

        return true;
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var subscriber = new Subscriber();
        T initial;
        long initialVersion;
        int newCount;
        MutableStateFlow<int>? subCounter;

        lock (_gate)
        {
            _subscribers.Add(subscriber);
            initial = _value;
            initialVersion = _version;
            newCount = _subscribers.Count;
            subCounter = _subscriptionCount;
        }
        if (subCounter is not null)
            subCounter.Value = newCount;

        try
        {
            yield return initial;

            T lastYielded = initial;
            long lastVersion = initialVersion;

            while (await subscriber.Channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (subscriber.Channel.Reader.TryRead(out _)) { }

                T latest;
                long latestVersion;
                lock (_gate)
                {
                    latest = _value;
                    latestVersion = _version;
                }

                if (latestVersion != lastVersion && !_comparer.Equals(lastYielded, latest))
                {
                    lastYielded = latest;
                    lastVersion = latestVersion;
                    yield return latest;
                }
            }
        }
        finally
        {
            int remaining;
            lock (_gate)
            {
                _subscribers.Remove(subscriber);
                remaining = _subscribers.Count;
                subCounter = _subscriptionCount;
            }
            if (subCounter is not null)
                subCounter.Value = remaining;
            subscriber.Channel.Writer.TryComplete();
        }
    }
}

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

    // ========================================================================
    // Time-Based Flow Operators (Debounce & Sample)
    // ========================================================================

    // Replicates Kotlin's `flow.debounce(timeout)`:
    // Filters out values that are followed by newer values within `timeout`.
    // Always emits the latest pending value if the upstream completes cleanly.
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

                // Flush trailing pending item upon clean upstream completion (matches Kotlin Flow.debounce)
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

    // Replicates Kotlin's `flow.sample(period)`:
    // Samples the latest emitted value during each periodic window, emitting only if a new value arrived.
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

    // ========================================================================
    // Exception Recovery Operators (Catch & RetryWhen)
    // ========================================================================

    // Replicates Kotlin's `flow.catch { cause -> emit(fallback) }`:
    // Catches upstream exceptions (excluding downstream cancellation) and executes a recovery handler.
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

    // Convenience overload for `flow.catch { cause -> emit(fallbackValue) }`
    public static IAsyncEnumerable<T> Catch<T>(
        this IAsyncEnumerable<T> source,
        Func<Exception, T> fallbackSelector)
    {
        return source.Catch(async (ex, writer, ct) =>
        {
            await writer.WriteAsync(fallbackSelector(ex), ct).ConfigureAwait(false);
        });
    }

    // Replicates Kotlin's `flow.retryWhen { cause, attempt -> ... }`:
    // Re-subscribes to the upstream flow whenever `predicate(exception, attemptIndex)` returns true.
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

    // Convenience overload for `flow.retry(retries) { cause -> ... }`
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

    // ========================================================================
    // Hot Stream Conversion Extensions (StateIn & ShareIn)
    // ========================================================================

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

// ============================================================================
// PART 4: Lua-style Multi-Shot Coroutines (.NET 11 / C# 15)
// ============================================================================

public static class Lua
{
    private static readonly AsyncLocal<object?> CurrentContext = new();

    internal static void SetCurrent(object? ctx) => CurrentContext.Value = ctx;

    public static CoroutineContext<TIn, TOut>.YieldAwaitable Yield<TIn, TOut>(TOut value)
    {
        if (CurrentContext.Value is not CoroutineContext<TIn, TOut> ctx)
            throw new InvalidOperationException("Lua.Yield must be called inside a matching LuaCoroutine<TIn, TOut>.");

        return ctx.Yield(value);
    }
}

public sealed class CoroutineContext<TIn, TOut>
{
    internal TIn CurrentInput = default!;
    internal TOut CurrentOutput = default!;
    internal Action? SavedContinuation;

    public YieldAwaitable Yield(TOut value)
    {
        CurrentOutput = value;
        return new YieldAwaitable(this);
    }

    public readonly struct YieldAwaitable : INotifyCompletion
    {
        private readonly CoroutineContext<TIn, TOut> _ctx;
        public YieldAwaitable(CoroutineContext<TIn, TOut> ctx) => _ctx = ctx;

        public bool IsCompleted => false;

        public void OnCompleted(Action continuation) => _ctx.SavedContinuation = continuation;

        public TIn GetResult() => _ctx.CurrentInput;
    }
}

[AsyncMethodBuilder(typeof(LuaCoroutineMethodBuilder<,>))]
public sealed class LuaCoroutine<TIn, TOut>
{
    internal readonly CoroutineContext<TIn, TOut> Context = new();
    internal IAsyncStateMachine? BoxedStateMachine;
    internal Action? MoveNextAction;
    internal bool Started;
    internal ExceptionDispatchInfo? Exception;

    public bool IsDead { get; internal set; }

    public (bool Success, TOut Value) Resume(TIn input = default!)
    {
        if (IsDead)
            return (false, default!);

        Context.CurrentInput = input;
        Lua.SetCurrent(Context);

        try
        {
            if (!Started)
            {
                Started = true;
                MoveNextAction?.Invoke();
            }
            else if (Context.SavedContinuation != null)
            {
                var next = Context.SavedContinuation;
                Context.SavedContinuation = null;
                next();
            }
        }
        finally
        {
            Lua.SetCurrent(null);
        }

        if (Exception != null)
        {
            IsDead = true;
            Exception.Throw();
        }

        return (true, Context.CurrentOutput);
    }
}

public struct LuaCoroutineMethodBuilder<TIn, TOut>
{
    private LuaCoroutine<TIn, TOut> _coroutine;

    public static LuaCoroutineMethodBuilder<TIn, TOut> Create()
        => new() { _coroutine = new LuaCoroutine<TIn, TOut>() };

    public LuaCoroutine<TIn, TOut> Task => _coroutine;

    public void Start<TStateMachine>(ref TStateMachine stateMachine)
        where TStateMachine : IAsyncStateMachine
    {
        IAsyncStateMachine boxed = stateMachine;
        _coroutine.BoxedStateMachine = boxed;
        _coroutine.MoveNextAction = boxed.MoveNext;
    }

    public void SetStateMachine(IAsyncStateMachine stateMachine) { }

    public void SetResult(TOut result)
    {
        _coroutine.Context.CurrentOutput = result;
        _coroutine.IsDead = true;
    }

    public void SetException(Exception exception)
    {
        _coroutine.Exception = ExceptionDispatchInfo.Capture(exception);
        _coroutine.IsDead = true;
    }

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter,
        ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        awaiter.OnCompleted(_coroutine.MoveNextAction!);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter,
        ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        awaiter.OnCompleted(_coroutine.MoveNextAction!);
    }
}

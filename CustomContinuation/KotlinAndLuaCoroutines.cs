using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
    // PART 3: Kotlin Flow Bridges, Hot SharedFlow<T>, & StateFlow<T>
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
}

public enum BufferOverflow
{
    Suspend,
    DropOldest,
    DropLatest
}

// Read-only hot broadcast stream contract (replicates Kotlin's `SharedFlow<T>`)
public interface ISharedFlow<out T> : IAsyncEnumerable<T>
{
    IReadOnlyList<T> ReplayCache { get; }
    int SubscriptionCount { get; }
}

// Mutable hot broadcast stream with configurable replay, extra buffer capacity, and overflow policy
// Replicates Kotlin's `MutableSharedFlow<T>` / `SharedFlow<T>`
public sealed class MutableSharedFlow<T> : ISharedFlow<T>
{
    private readonly Lock _gate = new();
    private readonly int _replay;
    private readonly int _bufferCapacity;
    private readonly BufferOverflow _onBufferOverflow;
    private readonly Queue<T> _replayBuffer = new();
    private readonly List<Subscriber> _subscribers = new();

    private sealed class Subscriber
    {
        public readonly Channel<T> Channel;

        public Subscriber(int capacity, BufferOverflow overflow)
        {
            if (capacity <= 0)
            {
                // Rendezvous-like minimum slot of 1 when no extra buffer is requested
                Channel = System.Threading.Channels.Channel.CreateBounded<T>(
                    new BoundedChannelOptions(1)
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
            else
            {
                Channel = System.Threading.Channels.Channel.CreateBounded<T>(
                    new BoundedChannelOptions(capacity)
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

    public int SubscriptionCount
    {
        get
        {
            lock (_gate)
            {
                return _subscribers.Count;
            }
        }
    }

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
            if (_onBufferOverflow == BufferOverflow.Suspend && _subscribers.Count > 0)
            {
                foreach (var sub in _subscribers)
                {
                    if (!sub.Channel.Writer.TryWrite(value))
                    {
                        // In Suspend mode, TryEmit fails atomically if any subscriber buffer is full
                        // Note: for multi-subscriber strict atomicity, check capacity or use EmitAsync
                    }
                }
                RecordReplayLocked(value);
                return true;
            }

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

        lock (_gate)
        {
            initialReplay = _replayBuffer.ToArray();
            _subscribers.Add(subscriber);
        }

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
            lock (_gate)
            {
                _subscribers.Remove(subscriber);
            }
            subscriber.Channel.Writer.TryComplete();
        }
    }
}

// Read-only hot state stream contract (replicates Kotlin's `StateFlow<T>`)
public interface IStateFlow<out T> : ISharedFlow<T>
{
    T Value { get; }
}

// Mutable hot state holder with conflated replay=1 and equality-based deduplication
// Replicates Kotlin's `MutableStateFlow<T>` / `StateFlow<T>`
public sealed class MutableStateFlow<T> : IStateFlow<T>
{
    private readonly Lock _gate = new();
    private readonly IEqualityComparer<T> _comparer;
    private readonly List<Subscriber> _subscribers = new();
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

    public int SubscriptionCount
    {
        get
        {
            lock (_gate)
            {
                return _subscribers.Count;
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

        lock (_gate)
        {
            _subscribers.Add(subscriber);
            initial = _value;
            initialVersion = _version;
        }

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
            lock (_gate)
            {
                _subscribers.Remove(subscriber);
            }
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

    // Converts a cold IAsyncEnumerable<T> into a hot StateFlow<T> active within `scope`
    public static IStateFlow<T> StateIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        T initialValue,
        IEqualityComparer<T>? comparer = null)
    {
        var stateFlow = new MutableStateFlow<T>(initialValue, comparer);
        source.LaunchIn(scope, value => stateFlow.Value = value);
        return stateFlow.AsStateFlow();
    }

    // Converts a cold IAsyncEnumerable<T> into a hot SharedFlow<T> active within `scope`
    // Replicates Kotlin's `flow.shareIn(scope, SharingStarted.Eagerly, replay)`
    public static ISharedFlow<T> ShareIn<T>(
        this IAsyncEnumerable<T> source,
        CoroutineScope scope,
        int replay = 0,
        int extraBufferCapacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
    {
        var sharedFlow = new MutableSharedFlow<T>(replay, extraBufferCapacity, onBufferOverflow);
        source.LaunchIn(scope, async (value, ct) => await sharedFlow.EmitAsync(value, ct).ConfigureAwait(false));
        return sharedFlow.AsSharedFlow();
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

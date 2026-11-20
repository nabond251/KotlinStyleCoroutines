using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
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

    // Scoped WithContext overloads for TaskScheduler (e.g., Dispatchers.Default, LimitedParallelism, SingleThread)
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
}

// ============================================================================
// PART 3: Lua-style Multi-Shot Coroutines (.NET 11 / C# 15)
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

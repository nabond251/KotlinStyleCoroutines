using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;

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

public static class Coroutine
{
    public static Suspendable<T> SuspendCancellable<T>(
        Action<Continuation<T>> block,
        CancellationToken token = default)
        => new(block, token);
}

// ============================================================================
// PART 2: Lua-style Multi-Shot Coroutines (.NET 11 / C# 15)
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
        // Box the state machine once so Release-mode struct state machines preserve state across yields
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

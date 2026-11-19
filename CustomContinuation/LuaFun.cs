using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

// 1. The Context passed into the coroutine body for yielding
public sealed class CoroutineContext<TIn, TOut>
{
    internal TIn CurrentInput = default!;
    internal TOut CurrentOutput = default!;
    internal Action? SavedContinuation;
    internal bool IsYielded;

    public YieldAwaitable Yield(TOut value)
    {
        CurrentOutput = value;
        IsYielded = true;
        return new YieldAwaitable(this);
    }

    public readonly struct YieldAwaitable : INotifyCompletion
    {
        private readonly CoroutineContext<TIn, TOut> _ctx;
        public YieldAwaitable(CoroutineContext<TIn, TOut> ctx) => _ctx = ctx;

        // Always suspend so control returns immediately to Resume()
        public bool IsCompleted => false;

        public void OnCompleted(Action continuation)
        {
            _ctx.SavedContinuation = continuation;
        }

        // Returns the new input passed into the next Resume(TIn) call
        public TIn GetResult() => _ctx.CurrentInput;
    }
}

// 2. The Lua-style Coroutine handle
[AsyncMethodBuilder(typeof(LuaCoroutineMethodBuilder<,>))]
public sealed class LuaCoroutine<TIn, TOut>
{
    internal readonly CoroutineContext<TIn, TOut> Context = new();
    internal Action? StartAction;
    internal ExceptionDispatchInfo? Exception;

    public bool IsDead { get; internal set; }

    public (bool Success, TOut Value) Resume(TIn input = default!)
    {
        if (IsDead)
            return (false, default!);

        Context.CurrentInput = input;
        Context.IsYielded = false;

        if (StartAction != null)
        {
            var start = StartAction;
            StartAction = null;
            start();
        }
        else if (Context.SavedContinuation != null)
        {
            var next = Context.SavedContinuation;
            Context.SavedContinuation = null;
            next();
        }

        if (Exception != null)
        {
            IsDead = true;
            Exception.Throw();
        }

        return (true, Context.CurrentOutput);
    }
}

// 3. Custom Compiler State Machine Builder
public struct LuaCoroutineMethodBuilder<TIn, TOut>
{
    private LuaCoroutine<TIn, TOut> _coroutine;

    public static LuaCoroutineMethodBuilder<TIn, TOut> Create()
        => new() { _coroutine = new LuaCoroutine<TIn, TOut>() };

    public LuaCoroutine<TIn, TOut> Task => _coroutine;

    public void Start<TStateMachine>(ref TStateMachine stateMachine)
        where TStateMachine : IAsyncStateMachine
    {
        // Defer initial execution until the first Resume() call, just like Lua's coroutine.create
        var sm = stateMachine;
        _coroutine.StartAction = () => sm.MoveNext();
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
        ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        awaiter.OnCompleted(stateMachine.MoveNext);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        awaiter.OnCompleted(stateMachine.MoveNext);
    }
}

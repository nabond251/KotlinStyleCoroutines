using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace KotlinStyleCoroutines;

/// <summary>
/// Lua-style Multi-Shot Coroutines (.NET 11 / C# 15)
/// </summary>
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

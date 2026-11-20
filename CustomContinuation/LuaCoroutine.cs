using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace KotlinStyleCoroutines;

/// <summary>
/// Lua-style Multi-Shot Coroutines (.NET 11 / C# 15)
/// </summary>
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

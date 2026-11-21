namespace KotlinStyleCoroutines;

/// <summary>
/// Lua-style Multi-Shot Coroutines (.NET 11 / C# 15)
/// </summary>
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

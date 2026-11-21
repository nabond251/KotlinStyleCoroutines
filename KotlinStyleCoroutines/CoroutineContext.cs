using System.Runtime.CompilerServices;

namespace KotlinStyleCoroutines;

/// <summary>
/// Lua-style Multi-Shot Coroutines (.NET 11 / C# 15)
/// </summary>
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

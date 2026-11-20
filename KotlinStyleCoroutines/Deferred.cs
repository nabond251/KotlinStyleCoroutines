using System.Runtime.CompilerServices;

namespace KotlinStyleCoroutines;

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

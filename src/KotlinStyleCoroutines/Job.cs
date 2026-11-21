using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace KotlinStyleCoroutines;

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

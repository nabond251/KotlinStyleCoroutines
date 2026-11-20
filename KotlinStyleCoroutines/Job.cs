using System.Runtime.CompilerServices;

namespace KotlinStyleCoroutines;

public sealed class Job : IDisposable
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

    // Replicates Kotlin's `job.cancel()`
    public void Cancel() => _cts.Cancel();

    // Replicates Kotlin's `job.join()` (awaits completion without rethrowing child exceptions if handled by scope)
    public Task JoinAsync() => _task;

    // Allows `await job;` directly in C#
    public TaskAwaiter GetAwaiter() => _task.GetAwaiter();

    public void Dispose() => _cts.Dispose();
}

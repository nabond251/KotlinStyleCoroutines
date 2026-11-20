using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;

namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Structured Concurrency (CoroutineScope & Coroutine.Scope)
/// </summary>
public sealed class CoroutineScope : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly ConcurrentBag<Task> _children = new();
    private ExceptionDispatchInfo? _firstException;

    public CancellationToken Token => _cts.Token;

    internal CoroutineScope(CancellationToken parentToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(parentToken);
    }

    public void Launch(Func<CancellationToken, Task> block)
    {
        _children.Add(RunChildAsync(block));
    }

    public Task<T> Async<T>(Func<CancellationToken, Task<T>> block)
    {
        var task = RunChildAsync(block);
        _children.Add(task);
        return task;
    }

    public void Cancel() => _cts.Cancel();

    internal Task CancelAsync() => _cts.CancelAsync();

    private async Task RunChildAsync(Func<CancellationToken, Task> block)
    {
        try
        {
            await block(_cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
            // Expected cancellation triggered by scope or parent
        }
        catch (Exception ex)
        {
            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
        }
    }

    private async Task<T> RunChildAsync<T>(Func<CancellationToken, Task<T>> block)
    {
        try
        {
            return await block(_cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
            throw;
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
                // Exceptions are already captured in _firstException or handled by callers
            }
        }

        _cts.Dispose();
        _firstException?.Throw();
    }
}

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
    private readonly bool _isSupervisor;
    private readonly Action<Exception>? _onChildException;
    private ExceptionDispatchInfo? _firstException;

    public CancellationToken Token => _cts.Token;

    internal CoroutineScope(
        CancellationToken parentToken,
        bool isSupervisor = false,
        Action<Exception>? onChildException = null)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(parentToken);
        _isSupervisor = isSupervisor;
        _onChildException = onChildException;
    }

    // Replicates Kotlin's `launch { ... }` (fire-and-track within scope)
    public void Launch(Func<CancellationToken, Task> block)
    {
        _children.Add(RunChildAsync(block));
    }

    // Replicates Kotlin's `async { ... }` (returns an awaitable Task<T> tracked by scope)
    public Task<T> Async<T>(Func<CancellationToken, Task<T>> block)
    {
        var task = RunChildAsync(block);
        _children.Add(task);
        return task;
    }

    public void Cancel() => _cts.Cancel();

    public Task CancelAsync() => _cts.CancelAsync();

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
            if (_isSupervisor)
            {
                // In a supervisor scope, child failures are isolated and do not cancel siblings or fail the scope
                _onChildException?.Invoke(ex);
                return;
            }

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
            if (_isSupervisor)
            {
                // Callers awaiting the returned Task<T> directly observe the exception; siblings keep running
                _onChildException?.Invoke(ex);
                throw;
            }

            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Wait for all launched and async children to finish or observe cancellation
        while (_children.TryTake(out var child))
        {
            try
            {
                await child.ConfigureAwait(false);
            }
            catch
            {
                // Exceptions are already captured in _firstException, reported to _onChildException, or awaited by callers
            }
        }

        _cts.Dispose();
        _firstException?.Throw();
    }
}

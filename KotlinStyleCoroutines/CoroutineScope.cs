using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;

namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Job, Deferred<T>, Dispatchers, & Structured Concurrency
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

    public Job Launch(Func<CancellationToken, Task> block)
    {
        var childCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var task = RunChildAsync(block, childCts);
        _children.Add(task);
        return new Job(childCts, task);
    }

    public Deferred<T> Async<T>(Func<CancellationToken, Task<T>> block)
    {
        var childCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var task = RunChildAsync(block, childCts);
        _children.Add(task);
        return new Deferred<T>(childCts, task);
    }

    public void Cancel() => _cts.Cancel();

    internal Task CancelAsync() => _cts.CancelAsync();

    private async Task RunChildAsync(Func<CancellationToken, Task> block, CancellationTokenSource childCts)
    {
        try
        {
            await block(childCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (childCts.IsCancellationRequested)
        {
            // Expected cancellation of this specific Job or its parent scope
        }
        catch (Exception ex)
        {
            if (_isSupervisor)
            {
                _onChildException?.Invoke(ex);
                return;
            }

            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
        }
        finally
        {
            childCts.Dispose();
        }
    }

    private async Task<T> RunChildAsync<T>(Func<CancellationToken, Task<T>> block, CancellationTokenSource childCts)
    {
        try
        {
            return await block(childCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (childCts.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (_isSupervisor)
            {
                _onChildException?.Invoke(ex);
                throw;
            }

            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            childCts.Dispose();
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
                // Exceptions are already captured in _firstException, reported to _onChildException, or awaited by callers
            }
        }

        _cts.Dispose();
        _firstException?.Throw();
    }
}

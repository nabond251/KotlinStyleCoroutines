using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;

namespace KotlinStyleCoroutines;

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

    // Replicates Kotlin's `launch { ... }` returning a cancellable/joinable Job
    public Job Launch(Func<CancellationToken, Task> block)
    {
        var childCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var task = RunChildAsync(block, childCts);
        _children.Add(task);
        return new Job(childCts, task);
    }

    // Parameterless overload relying on Coroutine.CurrentToken
    public Job Launch(Func<Task> block)
        => Launch(_ => block());

    // Replicates Kotlin's `async { ... }` returning a cancellable/awaitable Deferred<T>
    public Deferred<T> Async<T>(Func<CancellationToken, Task<T>> block)
    {
        var childCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var task = RunChildAsync(block, childCts);
        _children.Add(task);
        return new Deferred<T>(childCts, task);
    }

    // Parameterless overload relying on Coroutine.CurrentToken
    public Deferred<T> Async<T>(Func<Task<T>> block)
        => Async(_ => block());

    public void Cancel() => _cts.Cancel();

    public Task CancelAsync() => _cts.CancelAsync();

    private async Task RunChildAsync(Func<CancellationToken, Task> block, CancellationTokenSource childCts)
    {
        var previous = Coroutine.SwapAmbientToken(childCts.Token);
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
                // In a supervisor scope, child failures are isolated and do not cancel siblings or fail the scope
                _onChildException?.Invoke(ex);
                return;
            }

            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
        }
        finally
        {
            Coroutine.RestoreAmbientToken(previous);
            childCts.Dispose();
        }
    }

    private async Task<T> RunChildAsync<T>(Func<CancellationToken, Task<T>> block, CancellationTokenSource childCts)
    {
        var previous = Coroutine.SwapAmbientToken(childCts.Token);
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
                // Callers awaiting the returned Task<T> directly observe the exception; siblings keep running
                _onChildException?.Invoke(ex);
                throw;
            }

            Interlocked.CompareExchange(ref _firstException, ExceptionDispatchInfo.Capture(ex), null);
            await _cts.CancelAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            Coroutine.RestoreAmbientToken(previous);
            childCts.Dispose();
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
                // Exceptions are already captured in _firstException or handled by callers awaiting Deferred<T>
            }
        }

        _cts.Dispose();
        _firstException?.Throw();
    }
}

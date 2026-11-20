namespace KotlinStyleCoroutines;

﻿public static class Coroutine
{
    public static Suspendable<T> SuspendCancellable<T>(
        Action<Continuation<T>> block,
        CancellationToken token = default)
        => new(block, token);

    // Replicates Kotlin's `coroutineScope { ... }` suspending block
    public static async Task<T> Scope<T>(
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        await using var scope = new CoroutineScope(cancellationToken);
        try
        {
            return await block(scope).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            // If the main scope body throws, cancel all background children before DisposeAsync awaits them
            await scope.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    public static async Task Scope(
        Func<CoroutineScope, Task> block,
        CancellationToken cancellationToken = default)
    {
        await Scope<object?>(async s =>
        {
            await block(s).ConfigureAwait(false);
            return null;
        }, cancellationToken).ConfigureAwait(false);
    }

    // Replicates Kotlin's `supervisorScope { ... }`:
    // Child failures do not cancel sibling coroutines or fail the parent scope unless thrown directly from `block`.
    public static async Task<T> SupervisorScope<T>(
        Func<CoroutineScope, Task<T>> block,
        Action<Exception>? onUnhandledChildException = null,
        CancellationToken cancellationToken = default)
    {
        await using var scope = new CoroutineScope(
            cancellationToken,
            isSupervisor: true,
            onChildException: onUnhandledChildException);

        try
        {
            return await block(scope).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            await scope.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    public static async Task SupervisorScope(
        Func<CoroutineScope, Task> block,
        Action<Exception>? onUnhandledChildException = null,
        CancellationToken cancellationToken = default)
    {
        await SupervisorScope<object?>(async s =>
        {
            await block(s).ConfigureAwait(false);
            return null;
        }, onUnhandledChildException, cancellationToken).ConfigureAwait(false);
    }

    // Replicates `withContext(TaskScheduler)` (e.g., Dispatchers.Default, limited concurrency, or single-thread scheduler)
    public static async Task<T> WithContext<T>(
        TaskScheduler scheduler,
        Func<CancellationToken, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Task.Factory.StartNew sets TaskScheduler.Current for the entire async execution tree inside `block`
        return await await Task.Factory.StartNew(
            () => block(cancellationToken),
            cancellationToken,
            TaskCreationOptions.DenyChildAttach,
            scheduler).ConfigureAwait(false);
    }

    public static Task WithContext(
        TaskScheduler scheduler,
        Func<CancellationToken, Task> block,
        CancellationToken cancellationToken = default)
    {
        return WithContext<object?>(scheduler, async ct =>
        {
            await block(ct).ConfigureAwait(false);
            return null;
        }, cancellationToken);
    }

    // Replicates `withContext(SynchronizationContext)` (e.g., Dispatchers.Main / UI thread)
    public static async Task<T> WithContext<T>(
        SynchronizationContext targetContext,
        Func<CancellationToken, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        targetContext.Post(async static state =>
        {
            var (fn, ct, source) = ((Func<CancellationToken, Task<T>>, CancellationToken, TaskCompletionSource<T>))state!;
            try
            {
                ct.ThrowIfCancellationRequested();
                var result = await fn(ct); // Intentional: stay on targetContext across awaits inside `fn`
                source.TrySetResult(result);
            }
            catch (OperationCanceledException oce) when (ct.IsCancellationRequested)
            {
                source.TrySetCanceled(oce.CancellationToken);
            }
            catch (Exception ex)
            {
                source.TrySetException(ex);
            }
        }, (block, cancellationToken, tcs));

        return await tcs.Task.ConfigureAwait(false);
    }

    // Replicates `withContext(Dispatchers.Default) { this: CoroutineScope -> ... }`
    public static Task<T> WithContext<T>(
        TaskScheduler scheduler,
        Func<CoroutineScope, Task<T>> block,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // StartNew sets TaskScheduler.Current for the scope AND every child launched inside it
        return Task.Factory.StartNew(
            () => Scope(block, cancellationToken),
            cancellationToken,
            TaskCreationOptions.DenyChildAttach,
            scheduler).Unwrap();
    }

    public static Task WithContext(
        TaskScheduler scheduler,
        Func<CoroutineScope, Task> block,
        CancellationToken cancellationToken = default)
    {
        return WithContext<object?>(scheduler, async scope =>
        {
            await block(scope).ConfigureAwait(false);
            return null;
        }, cancellationToken);
    }
}

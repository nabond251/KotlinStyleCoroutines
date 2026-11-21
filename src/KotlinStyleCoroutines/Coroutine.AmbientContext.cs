namespace KotlinStyleCoroutines;

public static partial class Coroutine
{
    private static readonly AsyncLocal<CancellationToken> AmbientToken = new();

    /// <summary>
    /// Gets the active <see cref="CancellationToken"/> for the current structured coroutine scope,
    /// or <see cref="CancellationToken.None"/> if executing outside a scope or inside <see cref="NonCancellable{T}"/>.
    /// </summary>
    public static CancellationToken CurrentToken => AmbientToken.Value;

    /// <summary>
    /// Returns <see langword="true"/> when the current ambient coroutine context has not been canceled.
    /// Analogous to Kotlin's <c>coroutineContext.isActive</c>.
    /// </summary>
    public static bool IsActive => !AmbientToken.Value.IsCancellationRequested;

    /// <summary>
    /// Throws an <see cref="OperationCanceledException"/> if the current ambient coroutine context
    /// has been canceled. Analogous to Kotlin's <c>ensureActive()</c>.
    /// </summary>
    public static void EnsureActive() => AmbientToken.Value.ThrowIfCancellationRequested();

    /// <summary>
    /// Suspends for the specified <paramref name="delay"/> while observing <see cref="CurrentToken"/>
    /// (unless overridden by <paramref name="cancellationToken"/>). Analogous to Kotlin's <c>delay(ms)</c>.
    /// </summary>
    public static Task Delay(TimeSpan delay, CancellationToken cancellationToken = default)
        => Task.Delay(delay, ResolveToken(cancellationToken));

    /// <summary>
    /// Suspends for <paramref name="millisecondsDelay"/> while observing <see cref="CurrentToken"/>.
    /// Analogous to Kotlin's <c>delay(ms)</c>.
    /// </summary>
    public static Task Delay(int millisecondsDelay, CancellationToken cancellationToken = default)
        => Task.Delay(millisecondsDelay, ResolveToken(cancellationToken));

    /// <summary>
    /// Executes <paramref name="block"/> with <see cref="CurrentToken"/> temporarily reset to
    /// <see cref="CancellationToken.None"/> so cleanup logic in <c>finally</c> blocks can complete
    /// without immediately throwing on an already-canceled parent scope.
    /// Analogous to Kotlin's <c>withContext(NonCancellable) { ... }</c>.
    /// </summary>
    public static async Task<T> NonCancellable<T>(Func<Task<T>> block)
    {
        ArgumentNullException.ThrowIfNull(block);

        var previous = AmbientToken.Value;
        AmbientToken.Value = CancellationToken.None;
        try
        {
            return await block().ConfigureAwait(false);
        }
        finally
        {
            AmbientToken.Value = previous;
        }
    }

    /// <summary>
    /// Executes <paramref name="block"/> with <see cref="CurrentToken"/> temporarily reset to
    /// <see cref="CancellationToken.None"/>. Analogous to Kotlin's <c>withContext(NonCancellable) { ... }</c>.
    /// </summary>
    public static async Task NonCancellable(Func<Task> block)
    {
        ArgumentNullException.ThrowIfNull(block);

        var previous = AmbientToken.Value;
        AmbientToken.Value = CancellationToken.None;
        try
        {
            await block().ConfigureAwait(false);
        }
        finally
        {
            AmbientToken.Value = previous;
        }
    }

    internal static CancellationToken ResolveToken(CancellationToken explicitToken)
        => explicitToken.CanBeCanceled ? explicitToken : AmbientToken.Value;

    internal static CancellationToken SwapAmbientToken(CancellationToken next)
    {
        var previous = AmbientToken.Value;
        AmbientToken.Value = next;
        return previous;
    }

    internal static void RestoreAmbientToken(CancellationToken previous)
        => AmbientToken.Value = previous;
}

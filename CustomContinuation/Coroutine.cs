namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Single-Shot Custom Continuation (suspendCancellableCoroutine)
/// </summary>
public static class Coroutine
{
    public static Suspendable<T> SuspendCancellable<T>(
        Action<Continuation<T>> block,
        CancellationToken token = default)
        => new(block, token);
}

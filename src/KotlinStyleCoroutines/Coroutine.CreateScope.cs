namespace KotlinStyleCoroutines;

public static partial class Coroutine
{
    /// <summary>
    /// Creates a standalone <see cref="CoroutineScope"/> tied to <paramref name="parentToken"/>.
    /// Useful for UI components or services that manage their own mount/unmount lifecycle.
    /// </summary>
    public static CoroutineScope CreateScope(CancellationToken parentToken = default)
        => new(parentToken);
}

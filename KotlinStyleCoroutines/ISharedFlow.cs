namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Job, Deferred<T>, Dispatchers, & Structured Concurrency
/// </summary>
public interface ISharedFlow<out T> : IAsyncEnumerable<T>
{
    IReadOnlyList<T> ReplayCache { get; }
    IStateFlow<int> SubscriptionCount { get; }
}

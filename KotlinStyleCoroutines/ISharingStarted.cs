namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Job, Deferred<T>, Dispatchers, & Structured Concurrency
/// </summary>
public interface ISharingStarted
{
    IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount);
}

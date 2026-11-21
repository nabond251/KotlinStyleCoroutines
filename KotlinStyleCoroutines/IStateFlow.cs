namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Job, Deferred<T>, Dispatchers, & Structured Concurrency
/// </summary>
public interface IStateFlow<out T> : ISharedFlow<T>
{
    T Value { get; }
}

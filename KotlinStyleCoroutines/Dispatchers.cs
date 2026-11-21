namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Job, Deferred<T>, Dispatchers, & Structured Concurrency
/// </summary>
public static class Dispatchers
{
    // Equivalent to Dispatchers.Default / Dispatchers.IO
    public static TaskScheduler Default => TaskScheduler.Default;

    // Equivalent to Dispatchers.Main (captured on the UI thread at startup)
    public static SynchronizationContext? Main { get; set; }

    // Equivalent to Dispatchers.Default.limitedParallelism(maxConcurrency)
    public static TaskScheduler LimitedParallelism(int maxDegreeOfParallelism)
        => new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, maxDegreeOfParallelism).ConcurrentScheduler;

    // Equivalent to newSingleThreadContext("Worker")
    public static TaskScheduler SingleThread()
        => new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, maxConcurrencyLevel: 1).ExclusiveScheduler;
}

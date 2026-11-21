using System.Threading;
using System.Threading.Tasks;

namespace KotlinStyleCoroutines;

public static class Dispatchers
{
    public static TaskScheduler Default => TaskScheduler.Default;

    public static SynchronizationContext? Main { get; set; }

    public static TaskScheduler LimitedParallelism(int maxDegreeOfParallelism)
        => new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, maxDegreeOfParallelism).ConcurrentScheduler;

    public static TaskScheduler SingleThread()
        => new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, maxConcurrencyLevel: 1).ExclusiveScheduler;
}

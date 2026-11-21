using System.Runtime.CompilerServices;

namespace KotlinStyleCoroutines;

public enum BufferOverflow
{
    Suspend,
    DropOldest,
    DropLatest
}

public enum SharingCommand
{
    Start,
    Stop,
    StopAndResetReplayCache
}

public interface ISharingStarted
{
    IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount);
}

public static class SharingStarted
{
    public static ISharingStarted Eagerly { get; } = new EagerlySharingStarted();
    public static ISharingStarted Lazily { get; } = new LazilySharingStarted();

    public static ISharingStarted WhileSubscribed(
        TimeSpan? stopTimeout = null,
        TimeSpan? replayExpiration = null)
        => new WhileSubscribedSharingStarted(
            stopTimeout ?? TimeSpan.Zero,
            replayExpiration);

    private sealed class EagerlySharingStarted : ISharingStarted
    {
        public async IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount)
        {
            yield return SharingCommand.Start;
            await Task.CompletedTask;
        }
    }

    private sealed class LazilySharingStarted : ISharingStarted
    {
        public IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount)
            => CommandCore(subscriptionCount, CancellationToken.None);

        private static async IAsyncEnumerable<SharingCommand> CommandCore(
            IStateFlow<int> subscriptionCount,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (int count in subscriptionCount.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (count > 0)
                {
                    yield return SharingCommand.Start;
                    yield break;
                }
            }
        }
    }

    private sealed class WhileSubscribedSharingStarted : ISharingStarted
    {
        private readonly TimeSpan _stopTimeout;
        private readonly TimeSpan? _replayExpiration;

        public WhileSubscribedSharingStarted(TimeSpan stopTimeout, TimeSpan? replayExpiration)
        {
            _stopTimeout = stopTimeout;
            _replayExpiration = replayExpiration;
        }

        public IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount)
            => CommandCore(subscriptionCount, CancellationToken.None);

        private async IAsyncEnumerable<SharingCommand> CommandCore(
            IStateFlow<int> subscriptionCount,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var enumerator = subscriptionCount.GetAsyncEnumerator(cancellationToken);
            bool isRunning = false;
            CancellationTokenSource? stopCts = null;

            try
            {
                while (await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    int count = enumerator.Current;

                    if (count > 0)
                    {
                        if (stopCts is not null)
                        {
                            await stopCts.CancelAsync().ConfigureAwait(false);
                            stopCts.Dispose();
                            stopCts = null;
                        }

                        if (!isRunning)
                        {
                            isRunning = true;
                            yield return SharingCommand.Start;
                        }
                    }
                    else if (isRunning && stopCts is null)
                    {
                        if (_stopTimeout <= TimeSpan.Zero)
                        {
                            isRunning = false;
                            yield return SharingCommand.Stop;

                            if (_replayExpiration is { } exp && exp <= TimeSpan.Zero)
                            {
                                yield return SharingCommand.StopAndResetReplayCache;
                            }
                        }
                        else
                        {
                            stopCts = new CancellationTokenSource();
                            bool timedOut = false;

                            try
                            {
                                await Task.Delay(_stopTimeout, stopCts.Token).ConfigureAwait(false);
                                timedOut = true;
                            }
                            catch (OperationCanceledException)
                            {
                                // Resubscribed before stopTimeout elapsed
                            }

                            if (timedOut)
                            {
                                isRunning = false;
                                stopCts.Dispose();
                                stopCts = null;
                                yield return SharingCommand.Stop;

                                if (_replayExpiration is { } exp)
                                {
                                    if (exp > TimeSpan.Zero)
                                        await Task.Delay(exp, cancellationToken).ConfigureAwait(false);
                                    if (subscriptionCount.Value == 0)
                                        yield return SharingCommand.StopAndResetReplayCache;
                                }
                            }
                        }
                    }
                }
            }
            finally
            {
                stopCts?.Dispose();
            }
        }
    }
}

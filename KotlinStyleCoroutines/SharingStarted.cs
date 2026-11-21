using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

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
        public async IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount)
        {
            await foreach (int count in subscriptionCount.ConfigureAwait(false))
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

        public async IAsyncEnumerable<SharingCommand> Command(IStateFlow<int> subscriptionCount)
        {
            using var enumerator = subscriptionCount.GetAsyncEnumerator();
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
                            try
                            {
                                await Task.Delay(_stopTimeout, stopCts.Token).ConfigureAwait(false);
                                isRunning = false;
                                stopCts.Dispose();
                                stopCts = null;
                                yield return SharingCommand.Stop;

                                if (_replayExpiration is { } exp)
                                {
                                    if (exp > TimeSpan.Zero)
                                        await Task.Delay(exp).ConfigureAwait(false);
                                    if (subscriptionCount.Value == 0)
                                        yield return SharingCommand.StopAndResetReplayCache;
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                // Resubscribed before stopTimeout elapsed
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

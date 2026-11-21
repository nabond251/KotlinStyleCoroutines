using System.Threading.Channels;

namespace KotlinStyleCoroutines;

public interface ISharedFlow<out T> : IAsyncEnumerable<T>
{
    IReadOnlyList<T> ReplayCache { get; }
    IStateFlow<int> SubscriptionCount { get; }
}

public sealed class MutableSharedFlow<T> : ISharedFlow<T>
{
    private readonly Lock _gate = new();
    private readonly int _replay;
    private readonly int _bufferCapacity;
    private readonly BufferOverflow _onBufferOverflow;
    private readonly Queue<T> _replayBuffer = new();
    private readonly List<Subscriber> _subscribers = new();
    private readonly MutableStateFlow<int> _subscriptionCount = new(0);

    private sealed class Subscriber
    {
        public readonly Channel<T> Channel;

        public Subscriber(int capacity, BufferOverflow overflow)
        {
            int effectiveCapacity = capacity <= 0 ? 1 : capacity;
            Channel = System.Threading.Channels.Channel.CreateBounded<T>(
                new BoundedChannelOptions(effectiveCapacity)
                {
                    FullMode = overflow switch
                    {
                        BufferOverflow.DropOldest => BoundedChannelFullMode.DropOldest,
                        BufferOverflow.DropLatest => BoundedChannelFullMode.DropWrite,
                        _ => BoundedChannelFullMode.Wait
                    },
                    SingleReader = true,
                    SingleWriter = false
                });
        }
    }

    public MutableSharedFlow(
        int replay = 0,
        int extraBufferCapacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
    {
        if (replay < 0)
            throw new ArgumentOutOfRangeException(nameof(replay), "Replay must be non-negative.");
        if (extraBufferCapacity < 0)
            throw new ArgumentOutOfRangeException(nameof(extraBufferCapacity), "ExtraBufferCapacity must be non-negative.");

        _replay = replay;
        _bufferCapacity = replay + extraBufferCapacity;
        _onBufferOverflow = onBufferOverflow;
    }

    public IReadOnlyList<T> ReplayCache
    {
        get
        {
            lock (_gate)
            {
                return _replayBuffer.ToArray();
            }
        }
    }

    public IStateFlow<int> SubscriptionCount => _subscriptionCount;

    public ISharedFlow<T> AsSharedFlow() => this;

    public void ResetReplayCache()
    {
        lock (_gate)
        {
            _replayBuffer.Clear();
        }
    }

    public bool TryEmit(T value)
    {
        Subscriber[] snapshot;
        lock (_gate)
        {
            RecordReplayLocked(value);
            snapshot = _subscribers.ToArray();
        }

        bool allAccepted = true;
        foreach (var sub in snapshot)
        {
            if (!sub.Channel.Writer.TryWrite(value))
            {
                allAccepted = false;
            }
        }

        return allAccepted;
    }

    public async ValueTask EmitAsync(T value, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Subscriber[] snapshot;
        lock (_gate)
        {
            RecordReplayLocked(value);
            snapshot = _subscribers.ToArray();
        }

        foreach (var sub in snapshot)
        {
            if (_onBufferOverflow == BufferOverflow.Suspend)
            {
                await sub.Channel.Writer.WriteAsync(value, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                sub.Channel.Writer.TryWrite(value);
            }
        }
    }

    private void RecordReplayLocked(T value)
    {
        if (_replay <= 0)
            return;

        if (_replayBuffer.Count == _replay)
            _replayBuffer.Dequeue();

        _replayBuffer.Enqueue(value);
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var subscriber = new Subscriber(_bufferCapacity, _onBufferOverflow);
        T[] initialReplay;
        int newCount;

        lock (_gate)
        {
            initialReplay = _replayBuffer.ToArray();
            _subscribers.Add(subscriber);
            newCount = _subscribers.Count;
        }
        _subscriptionCount.Value = newCount;

        try
        {
            foreach (var cached in initialReplay)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return cached;
            }

            await foreach (var item in subscriber.Channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            int remaining;
            lock (_gate)
            {
                _subscribers.Remove(subscriber);
                remaining = _subscribers.Count;
            }
            _subscriptionCount.Value = remaining;
            subscriber.Channel.Writer.TryComplete();
        }
    }
}

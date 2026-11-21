using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KotlinStyleCoroutines;

public interface ISharedFlow<out T> : IAsyncEnumerable<T>
{
    IReadOnlyList<T> ReplayCache { get; }
    IStateFlow<int> SubscriptionCount { get; }
}

public sealed class MutableSharedFlow<T> : ISharedFlow<T>
{
    private readonly int _replay;
    private readonly int _extraBufferCapacity;
    private readonly BufferOverflow _onBufferOverflow;
    private readonly Lock _gate = new();
    private readonly LinkedList<T> _replayBuffer = new();
    private readonly List<Subscriber> _subscribers = [];
    private readonly MutableStateFlow<int> _subscriptionCount = new(0);

    public MutableSharedFlow(
        int replay = 0,
        int extraBufferCapacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
    {
        if (replay < 0)
            throw new ArgumentOutOfRangeException(nameof(replay), "Replay cannot be negative.");
        if (extraBufferCapacity < 0)
            throw new ArgumentOutOfRangeException(nameof(extraBufferCapacity), "Extra buffer capacity cannot be negative.");

        _replay = replay;
        _extraBufferCapacity = extraBufferCapacity;
        _onBufferOverflow = onBufferOverflow;
    }

    public IStateFlow<int> SubscriptionCount => _subscriptionCount.AsStateFlow();

    public IReadOnlyList<T> ReplayCache
    {
        get
        {
            lock (_gate)
            {
                return [.. _replayBuffer];
            }
        }
    }

    public ISharedFlow<T> AsSharedFlow() => this;

    public bool TryEmit(T value)
    {
        lock (_gate)
        {
            if (_onBufferOverflow == BufferOverflow.Suspend && _subscribers.Count > 0)
            {
                foreach (var sub in _subscribers)
                {
                    if (!sub.CanAcceptWithoutBlocking())
                        return false;
                }
            }

            AppendToReplayLocked(value);

            foreach (var sub in _subscribers)
            {
                sub.EnqueueLocked(value);
            }

            return true;
        }
    }

    public async ValueTask EmitAsync(T value, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (TryEmit(value))
            return;

        List<ChannelWriter<T>> writers;
        lock (_gate)
        {
            AppendToReplayLocked(value);
            writers = new List<ChannelWriter<T>>(_subscribers.Count);
            foreach (var sub in _subscribers)
            {
                writers.Add(sub.Writer);
            }
        }

        foreach (var writer in writers)
        {
            await writer.WriteAsync(value, cancellationToken).ConfigureAwait(false);
        }
    }

    public void ResetReplayCache()
    {
        lock (_gate)
        {
            _replayBuffer.Clear();
        }
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var subscriber = new Subscriber(_replay + _extraBufferCapacity, _onBufferOverflow);
        List<T> initialReplay;
        int newCount;

        lock (_gate)
        {
            initialReplay = [.. _replayBuffer];
            _subscribers.Add(subscriber);
            newCount = _subscribers.Count;
        }

        _subscriptionCount.Value = newCount;

        try
        {
            foreach (var item in initialReplay)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }

            await foreach (var item in subscriber.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
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
            subscriber.Complete();
            _subscriptionCount.Value = remaining;
        }
    }

    private void AppendToReplayLocked(T value)
    {
        if (_replay == 0)
            return;

        if (_replayBuffer.Count == _replay)
            _replayBuffer.RemoveFirst();

        _replayBuffer.AddLast(value);
    }

    private sealed class Subscriber
    {
        private readonly Channel<T> _channel;
        private readonly BufferOverflow _overflow;
        private readonly int _capacity;

        public Subscriber(int capacity, BufferOverflow overflow)
        {
            _overflow = overflow;
            _capacity = Math.Max(1, capacity);

            var mode = overflow switch
            {
                BufferOverflow.DropOldest => BoundedChannelFullMode.DropOldest,
                BufferOverflow.DropLatest => BoundedChannelFullMode.DropWrite,
                _ => BoundedChannelFullMode.Wait
            };

            _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(_capacity)
            {
                FullMode = mode,
                SingleReader = true,
                SingleWriter = false
            });
        }

        public ChannelReader<T> Reader => _channel.Reader;
        public ChannelWriter<T> Writer => _channel.Writer;

        public bool CanAcceptWithoutBlocking()
            => _overflow != BufferOverflow.Suspend || _channel.Reader.Count < _capacity;

        public void EnqueueLocked(T value)
        {
            _channel.Writer.TryWrite(value);
        }

        public void Complete()
        {
            _channel.Writer.TryComplete();
        }
    }
}

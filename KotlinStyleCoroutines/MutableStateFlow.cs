using System.Threading.Channels;

namespace KotlinStyleCoroutines;

/// <summary>
/// Kotlin-style Job, Deferred<T>, Dispatchers, & Structured Concurrency
/// </summary>
public sealed class MutableStateFlow<T> : IStateFlow<T>
{
    private readonly Lock _gate = new();
    private readonly IEqualityComparer<T> _comparer;
    private readonly List<Subscriber> _subscribers = new();
    private MutableStateFlow<int>? _subscriptionCount;
    private T _value;
    private long _version;

    private sealed class Subscriber
    {
        public readonly Channel<T> Channel = System.Threading.Channels.Channel.CreateBounded<T>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });
    }

    public MutableStateFlow(T initialValue, IEqualityComparer<T>? comparer = null)
    {
        _value = initialValue;
        _comparer = comparer ?? EqualityComparer<T>.Default;
    }

    public T Value
    {
        get
        {
            lock (_gate)
            {
                return _value;
            }
        }
        set => TryEmit(value);
    }

    public IReadOnlyList<T> ReplayCache
    {
        get
        {
            lock (_gate)
            {
                return [_value];
            }
        }
    }

    public IStateFlow<int> SubscriptionCount
    {
        get
        {
            lock (_gate)
            {
                return _subscriptionCount ??= new MutableStateFlow<int>(_subscribers.Count);
            }
        }
    }

    public IStateFlow<T> AsStateFlow() => this;

    public bool TryEmit(T newValue)
    {
        Subscriber[] snapshot;
        lock (_gate)
        {
            if (_comparer.Equals(_value, newValue))
                return false;

            _value = newValue;
            _version++;
            snapshot = _subscribers.ToArray();
        }

        foreach (var sub in snapshot)
        {
            sub.Channel.Writer.TryWrite(newValue);
        }

        return true;
    }

    public T UpdateAndGet(Func<T, T> transform)
    {
        while (true)
        {
            T prev;
            lock (_gate)
            {
                prev = _value;
            }

            T next = transform(prev);
            if (CompareAndSet(prev, next))
                return next;
        }
    }

    public T GetAndUpdate(Func<T, T> transform)
    {
        while (true)
        {
            T prev;
            lock (_gate)
            {
                prev = _value;
            }

            T next = transform(prev);
            if (CompareAndSet(prev, next))
                return prev;
        }
    }

    public bool CompareAndSet(T expect, T update)
    {
        Subscriber[] snapshot;
        lock (_gate)
        {
            if (!_comparer.Equals(_value, expect))
                return false;

            if (_comparer.Equals(_value, update))
                return true;

            _value = update;
            _version++;
            snapshot = _subscribers.ToArray();
        }

        foreach (var sub in snapshot)
        {
            sub.Channel.Writer.TryWrite(update);
        }

        return true;
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var subscriber = new Subscriber();
        T initial;
        long initialVersion;
        int newCount;
        MutableStateFlow<int>? subCounter;

        lock (_gate)
        {
            _subscribers.Add(subscriber);
            initial = _value;
            initialVersion = _version;
            newCount = _subscribers.Count;
            subCounter = _subscriptionCount;
        }
        if (subCounter is not null)
            subCounter.Value = newCount;

        try
        {
            yield return initial;

            T lastYielded = initial;
            long lastVersion = initialVersion;

            while (await subscriber.Channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (subscriber.Channel.Reader.TryRead(out _)) { }

                T latest;
                long latestVersion;
                lock (_gate)
                {
                    latest = _value;
                    latestVersion = _version;
                }

                if (latestVersion != lastVersion && !_comparer.Equals(lastYielded, latest))
                {
                    lastYielded = latest;
                    lastVersion = latestVersion;
                    yield return latest;
                }
            }
        }
        finally
        {
            int remaining;
            lock (_gate)
            {
                _subscribers.Remove(subscriber);
                remaining = _subscribers.Count;
                subCounter = _subscriptionCount;
            }
            if (subCounter is not null)
                subCounter.Value = remaining;
            subscriber.Channel.Writer.TryComplete();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KotlinStyleCoroutines;

public interface IStateFlow<out T> : ISharedFlow<T>
{
    T Value { get; }
}

public sealed class MutableStateFlow<T> : IStateFlow<T>
{
    private readonly IEqualityComparer<T> _comparer;
    private readonly Lock _gate = new();
    private readonly List<ChannelWriter<T>> _subscribers = [];
    private T _value;

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

    public IReadOnlyList<T> ReplayCache => [Value];

    public IStateFlow<int> SubscriptionCount => field ??= new MutableStateFlow<int>(0);

    public IStateFlow<T> AsStateFlow() => this;

    public bool TryEmit(T value)
    {
        lock (_gate)
        {
            if (_comparer.Equals(_value, value))
                return false;

            _value = value;

            foreach (var sub in _subscribers)
            {
                sub.TryWrite(value);
            }

            return true;
        }
    }

    public ValueTask EmitAsync(T value, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        TryEmit(value);
        return ValueTask.CompletedTask;
    }

    public bool CompareAndSet(T expect, T update)
    {
        lock (_gate)
        {
            if (!_comparer.Equals(_value, expect))
                return false;

            if (!_comparer.Equals(_value, update))
            {
                _value = update;
                foreach (var sub in _subscribers)
                {
                    sub.TryWrite(update);
                }
            }

            return true;
        }
    }

    public T UpdateAndGet(Func<T, T> function)
    {
        lock (_gate)
        {
            T next = function(_value);
            if (!_comparer.Equals(_value, next))
            {
                _value = next;
                foreach (var sub in _subscribers)
                {
                    sub.TryWrite(next);
                }
            }
            return _value;
        }
    }

    public T GetAndUpdate(Func<T, T> function)
    {
        lock (_gate)
        {
            T prev = _value;
            T next = function(prev);
            if (!_comparer.Equals(prev, next))
            {
                _value = next;
                foreach (var sub in _subscribers)
                {
                    sub.TryWrite(next);
                }
            }
            return prev;
        }
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

        T initial;
        int newCount;
        lock (_gate)
        {
            initial = _value;
            _subscribers.Add(channel.Writer);
            newCount = _subscribers.Count;
        }

        UpdateSubscriptionCount(newCount);

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return initial;

            await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            int remaining;
            lock (_gate)
            {
                _subscribers.Remove(channel.Writer);
                remaining = _subscribers.Count;
            }
            channel.Writer.TryComplete();
            UpdateSubscriptionCount(remaining);
        }
    }

    private void UpdateSubscriptionCount(int count)
    {
        if (SubscriptionCount is MutableStateFlow<int> counter)
        {
            counter.Value = count;
        }
    }
}

using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace KotlinStyleCoroutines;

/// <summary>
/// Capacity constants for Kotlin-style channel builders (<see cref="ChannelBuilders.Produce{T}"/> and <see cref="ChannelBuilders.Actor{T}"/>).
/// </summary>
public static class ChannelCapacity
{
    /// <summary>
    /// Requests an unbounded channel buffer.
    /// </summary>
    public const int Unbounded = -1;

    /// <summary>
    /// Requests a single-slot conflated channel that drops the oldest unread item when full.
    /// </summary>
    public const int Conflated = -2;
}

/// <summary>
/// Scope passed to <see cref="ChannelBuilders.Produce{T}"/> blocks, combining structured child launching
/// with a <see cref="ChannelWriter{T}"/> to send items downstream.
/// </summary>
/// <typeparam name="T">The type of elements written to the channel.</typeparam>
public sealed class ProducerScope<T>
{
    private readonly CoroutineScope _innerScope;

    internal ProducerScope(CoroutineScope innerScope, ChannelWriter<T> channel)
    {
        _innerScope = innerScope;
        Channel = channel;
    }

    /// <summary>
    /// Gets the underlying <see cref="ChannelWriter{T}"/> used to send elements.
    /// </summary>
    public ChannelWriter<T> Channel { get; }

    /// <summary>
    /// Gets the cancellation token bound to this producer coroutine.
    /// </summary>
    public CancellationToken Token => _innerScope.Token;

    /// <summary>
    /// Launches a tracked child coroutine inside this producer scope.
    /// </summary>
    public Job Launch(Func<CancellationToken, Task> block)
        => _innerScope.Launch(block);

    /// <summary>
    /// Launches a tracked child coroutine inside this producer scope using ambient cancellation.
    /// </summary>
    public Job Launch(Func<Task> block)
        => _innerScope.Launch(block);

    /// <summary>
    /// Starts a tracked child computation returning a <see cref="Deferred{TResult}"/>.
    /// </summary>
    public Deferred<TResult> Async<TResult>(Func<CancellationToken, Task<TResult>> block)
        => _innerScope.Async(block);

    /// <summary>
    /// Starts a tracked child computation returning a <see cref="Deferred{TResult}"/> using ambient cancellation.
    /// </summary>
    public Deferred<TResult> Async<TResult>(Func<Task<TResult>> block)
        => _innerScope.Async(block);

    /// <summary>
    /// Writes an item to the channel asynchronously, suspending if the buffer is full.
    /// </summary>
    public ValueTask SendAsync(T item, CancellationToken cancellationToken = default)
        => Channel.WriteAsync(item, Coroutine.ResolveToken(cancellationToken));

    /// <summary>
    /// Attempts to write an item to the channel immediately without suspending.
    /// </summary>
    public bool TrySend(T item) => Channel.TryWrite(item);
}

/// <summary>
/// Represents a running producer coroutine and its readable <see cref="ChannelReader{T}"/>.
/// </summary>
/// <typeparam name="T">The type of elements produced by the channel.</typeparam>
public sealed class ProducedChannel<T> : IAsyncEnumerable<T>
{
    internal ProducedChannel(ChannelReader<T> reader, Job job)
    {
        Reader = reader;
        Job = job;
    }

    /// <summary>
    /// Gets the <see cref="ChannelReader{T}"/> for consuming produced elements.
    /// </summary>
    public ChannelReader<T> Reader { get; }

    /// <summary>
    /// Gets the <see cref="Job"/> controlling the lifecycle of the producer coroutine.
    /// </summary>
    public Job Job { get; }

    /// <summary>
    /// Cancels the producer coroutine.
    /// </summary>
    public void Cancel() => Job.Cancel();

    /// <summary>
    /// Consumes all elements produced by the channel until it completes.
    /// </summary>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var token = Coroutine.ResolveToken(cancellationToken);
        await foreach (var item in Reader.ReadAllAsync(token).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>
    /// Allows <c>await producedChannel.Reader.ReadAsync()</c> or implicit conversion to <see cref="ChannelReader{T}"/>.
    /// </summary>
    public static implicit operator ChannelReader<T>(ProducedChannel<T> produced)
    {
        ArgumentNullException.ThrowIfNull(produced);
        return produced.Reader;
    }
}

/// <summary>
/// Scope passed to <see cref="ChannelBuilders.Actor{T}"/> blocks, providing a <see cref="ChannelReader{T}"/>
/// mailbox and structured child launching.
/// </summary>
/// <typeparam name="T">The type of messages received by the actor.</typeparam>
public sealed class ActorScope<T>
{
    private readonly CoroutineScope _innerScope;

    internal ActorScope(CoroutineScope innerScope, ChannelReader<T> channel)
    {
        _innerScope = innerScope;
        Channel = channel;
    }

    /// <summary>
    /// Gets the actor's incoming <see cref="ChannelReader{T}"/> mailbox.
    /// </summary>
    public ChannelReader<T> Channel { get; }

    /// <summary>
    /// Gets the cancellation token bound to this actor coroutine.
    /// </summary>
    public CancellationToken Token => _innerScope.Token;

    /// <summary>
    /// Launches a tracked child coroutine inside this actor scope.
    /// </summary>
    public Job Launch(Func<CancellationToken, Task> block)
        => _innerScope.Launch(block);

    /// <summary>
    /// Launches a tracked child coroutine inside this actor scope using ambient cancellation.
    /// </summary>
    public Job Launch(Func<Task> block)
        => _innerScope.Launch(block);

    /// <summary>
    /// Starts a tracked child computation returning a <see cref="Deferred{TResult}"/>.
    /// </summary>
    public Deferred<TResult> Async<TResult>(Func<CancellationToken, Task<TResult>> block)
        => _innerScope.Async(block);

    /// <summary>
    /// Starts a tracked child computation returning a <see cref="Deferred{TResult}"/> using ambient cancellation.
    /// </summary>
    public Deferred<TResult> Async<TResult>(Func<Task<TResult>> block)
        => _innerScope.Async(block);

    /// <summary>
    /// Enumerates all incoming messages from the actor mailbox until the channel is closed.
    /// </summary>
    public IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default)
        => Channel.ReadAllAsync(Coroutine.ResolveToken(cancellationToken));
}

/// <summary>
/// Represents a running actor coroutine and its writable <see cref="ChannelWriter{T}"/> mailbox.
/// </summary>
/// <typeparam name="T">The type of messages accepted by the actor.</typeparam>
public sealed class ActorChannel<T>
{
    internal ActorChannel(ChannelWriter<T> writer, Job job)
    {
        Writer = writer;
        Job = job;
    }

    /// <summary>
    /// Gets the <see cref="ChannelWriter{T}"/> mailbox used to send messages to the actor.
    /// </summary>
    public ChannelWriter<T> Writer { get; }

    /// <summary>
    /// Gets the <see cref="Job"/> controlling the lifecycle of the actor coroutine.
    /// </summary>
    public Job Job { get; }

    /// <summary>
    /// Sends a message to the actor mailbox asynchronously.
    /// </summary>
    public ValueTask SendAsync(T message, CancellationToken cancellationToken = default)
        => Writer.WriteAsync(message, Coroutine.ResolveToken(cancellationToken));

    /// <summary>
    /// Attempts to enqueue a message immediately without suspending.
    /// </summary>
    public bool TrySend(T message) => Writer.TryWrite(message);

    /// <summary>
    /// Closes the actor mailbox so no further messages are accepted and the actor drains remaining items.
    /// </summary>
    public bool Close(Exception? error = null) => Writer.TryComplete(error);

    /// <summary>
    /// Cancels the actor coroutine immediately.
    /// </summary>
    public void Cancel() => Job.Cancel();

    /// <summary>
    /// Allows passing an <see cref="ActorChannel{T}"/> directly wherever a <see cref="ChannelWriter{T}"/> is expected.
    /// </summary>
    public static implicit operator ChannelWriter<T>(ActorChannel<T> actor)
    {
        ArgumentNullException.ThrowIfNull(actor);
        return actor.Writer;
    }
}

/// <summary>
/// Kotlin-style <c>produce</c> and <c>actor</c> structured channel builder extensions on <see cref="CoroutineScope"/>.
/// </summary>
public static class ChannelBuilders
{
    /// <summary>
    /// Launches a structured producer coroutine that emits elements into a bounded or unbounded channel
    /// and automatically closes the channel when <paramref name="block"/> completes or throws.
    /// </summary>
    public static ProducedChannel<T> Produce<T>(
        this CoroutineScope scope,
        Func<ProducerScope<T>, Task> block,
        int capacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(block);

        var channel = CreateChannel<T>(capacity, onBufferOverflow, singleWriter: false, singleReader: false);

        var job = scope.Launch(async ct =>
        {
            Exception? failure = null;
            try
            {
                await Coroutine.Scope(async innerScope =>
                {
                    var producerScope = new ProducerScope<T>(innerScope, channel.Writer);
                    await block(producerScope).ConfigureAwait(false);
                }, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                failure = ex;
                throw;
            }
            finally
            {
                channel.Writer.TryComplete(failure);
            }
        });

        return new ProducedChannel<T>(channel.Reader, job);
    }

    /// <summary>
    /// Launches a structured actor coroutine backed by a single-reader mailbox channel.
    /// </summary>
    public static ActorChannel<T> Actor<T>(
        this CoroutineScope scope,
        Func<ActorScope<T>, Task> block,
        int capacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(block);

        var channel = CreateChannel<T>(capacity, onBufferOverflow, singleWriter: false, singleReader: true);

        var job = scope.Launch(async ct =>
        {
            try
            {
                await Coroutine.Scope(async innerScope =>
                {
                    var actorScope = new ActorScope<T>(innerScope, channel.Reader);
                    await block(actorScope).ConfigureAwait(false);
                }, ct).ConfigureAwait(false);
            }
            finally
            {
                channel.Writer.TryComplete();
            }
        });

        return new ActorChannel<T>(channel.Writer, job);
    }

    internal static Channel<T> CreateChannel<T>(
        int capacity,
        BufferOverflow onBufferOverflow,
        bool singleWriter,
        bool singleReader)
    {
        if (capacity == ChannelCapacity.Unbounded)
        {
            return Channel.CreateUnbounded<T>(new UnboundedChannelOptions
            {
                SingleWriter = singleWriter,
                SingleReader = singleReader
            });
        }

        if (capacity == ChannelCapacity.Conflated)
        {
            return Channel.CreateBounded<T>(new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleWriter = singleWriter,
                SingleReader = singleReader
            });
        }

        // Rendezvous (capacity == 0) maps to a 1-slot bounded channel in System.Threading.Channels
        var effectiveCapacity = capacity <= 0 ? 1 : capacity;
        var fullMode = onBufferOverflow switch
        {
            BufferOverflow.Suspend => BoundedChannelFullMode.Wait,
            BufferOverflow.DropOldest => BoundedChannelFullMode.DropOldest,
            BufferOverflow.DropLatest => BoundedChannelFullMode.DropWrite,
            _ => throw new ArgumentOutOfRangeException(nameof(onBufferOverflow))
        };

        return Channel.CreateBounded<T>(new BoundedChannelOptions(effectiveCapacity)
        {
            FullMode = fullMode,
            SingleWriter = singleWriter,
            SingleReader = singleReader
        });
    }
}

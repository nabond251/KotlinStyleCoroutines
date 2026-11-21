using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace KotlinStyleCoroutines;

/// <summary>
/// Scope passed to a <c>Produce</c> coroutine builder, combining structured child launching
/// with a <see cref="ChannelWriter{T}"/> to send elements downstream.
/// </summary>
public sealed class ProducerScope<T>
{
    private readonly CoroutineScope _scope;

    internal ProducerScope(CoroutineScope scope, ChannelWriter<T> channel)
    {
        _scope = scope;
        Channel = channel;
    }

    /// <summary>Gets the underlying channel writer.</summary>
    public ChannelWriter<T> Channel { get; }

    /// <summary>Gets the cancellation token tied to this producer coroutine.</summary>
    public CancellationToken Token => _scope.Token;

    /// <summary>Sends an element to the channel, suspending if the buffer is full.</summary>
    public ValueTask SendAsync(T item, CancellationToken cancellationToken = default)
        => Channel.WriteAsync(item, cancellationToken);

    /// <summary>Attempts to send an element to the channel without suspending.</summary>
    public bool TrySend(T item) => Channel.TryWrite(item);

    /// <summary>Launches a structured child coroutine inside this producer's scope.</summary>
    public Job Launch(Func<CancellationToken, Task> block, TaskScheduler? scheduler = null)
        => _scope.Launch(block, scheduler);

    /// <summary>Launches a structured child coroutine returning a <see cref="Deferred{TResult}"/>.</summary>
    public Deferred<TResult> Async<TResult>(Func<CancellationToken, Task<TResult>> block, TaskScheduler? scheduler = null)
        => _scope.Async(block, scheduler);
}

/// <summary>
/// Represents a cold-to-hot channel stream created by <see cref="ChannelBuilderExtensions.Produce{T}"/>,
/// pairing a <see cref="ChannelReader{T}"/> with the producer's lifecycle <see cref="Job"/>.
/// </summary>
public sealed class ProducedChannel<T>
{
    private readonly Channel<T> _channel;

    internal ProducedChannel(Channel<T> channel, Job producerJob)
    {
        _channel = channel;
        ProducerJob = producerJob;
    }

    /// <summary>Gets the underlying channel reader.</summary>
    public ChannelReader<T> Reader => _channel.Reader;

    /// <summary>Gets the <see cref="Job"/> running the producer coroutine.</summary>
    public Job ProducerJob { get; }

    /// <summary>Gets whether the producer coroutine is still active.</summary>
    public bool IsActive => ProducerJob.IsActive;

    /// <summary>Reads the next element from the channel.</summary>
    public ValueTask<T> ReceiveAsync(CancellationToken cancellationToken = default)
        => _channel.Reader.ReadAsync(cancellationToken);

    /// <summary>Attempts to read an element without suspending.</summary>
    public bool TryReceive(out T? item) => _channel.Reader.TryRead(out item);

    /// <summary>Asynchronously enumerates all elements until the producer completes the channel.</summary>
    public IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default)
        => _channel.Reader.ReadAllAsync(cancellationToken);

    /// <summary>Cancels the producer coroutine and closes the channel.</summary>
    public void Cancel()
    {
        ProducerJob.Cancel();
        _channel.Writer.TryComplete(new OperationCanceledException("ProducedChannel was canceled."));
    }

    /// <summary>Awaits completion of the producer coroutine.</summary>
    public Task JoinAsync() => ProducerJob.JoinAsync();

    /// <summary>Allows awaiting the producer's completion directly.</summary>
    public TaskAwaiter GetAwaiter() => ProducerJob.GetAwaiter();
}

/// <summary>
/// Scope passed to an <c>Actor</c> coroutine builder, providing a mailbox <see cref="ChannelReader{T}"/>
/// alongside structured child launching.
/// </summary>
public sealed class ActorScope<T>
{
    private readonly CoroutineScope _scope;

    internal ActorScope(CoroutineScope scope, ChannelReader<T> channel)
    {
        _scope = scope;
        Channel = channel;
    }

    /// <summary>Gets the actor's incoming mailbox reader.</summary>
    public ChannelReader<T> Channel { get; }

    /// <summary>Gets the cancellation token tied to this actor coroutine.</summary>
    public CancellationToken Token => _scope.Token;

    /// <summary>Receives the next message from the actor's mailbox.</summary>
    public ValueTask<T> ReceiveAsync(CancellationToken cancellationToken = default)
        => Channel.ReadAsync(cancellationToken);

    /// <summary>Attempts to read a message from the mailbox without suspending.</summary>
    public bool TryReceive(out T? item) => Channel.TryRead(out item);

    /// <summary>Asynchronously enumerates incoming messages until the mailbox is closed.</summary>
    public IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default)
        => Channel.ReadAllAsync(cancellationToken);

    /// <summary>Launches a structured child coroutine inside this actor's scope.</summary>
    public Job Launch(Func<CancellationToken, Task> block, TaskScheduler? scheduler = null)
        => _scope.Launch(block, scheduler);

    /// <summary>Launches a structured child coroutine returning a <see cref="Deferred{TResult}"/>.</summary>
    public Deferred<TResult> Async<TResult>(Func<CancellationToken, Task<TResult>> block, TaskScheduler? scheduler = null)
        => _scope.Async(block, scheduler);
}

/// <summary>
/// Represents a stateful actor coroutine created by <see cref="ChannelBuilderExtensions.Actor{T}"/>,
/// exposing a mailbox <see cref="ChannelWriter{T}"/> and the actor's lifecycle <see cref="Job"/>.
/// </summary>
public sealed class ActorChannel<T>
{
    private readonly Channel<T> _channel;

    internal ActorChannel(Channel<T> channel, Job actorJob)
    {
        _channel = channel;
        ActorJob = actorJob;
    }

    /// <summary>Gets the underlying mailbox writer.</summary>
    public ChannelWriter<T> Writer => _channel.Writer;

    /// <summary>Gets the <see cref="Job"/> running the actor coroutine.</summary>
    public Job ActorJob { get; }

    /// <summary>Gets whether the actor coroutine is still active.</summary>
    public bool IsActive => ActorJob.IsActive;

    /// <summary>Sends a message to the actor's mailbox, suspending if the mailbox buffer is full.</summary>
    public ValueTask SendAsync(T message, CancellationToken cancellationToken = default)
        => _channel.Writer.WriteAsync(message, cancellationToken);

    /// <summary>Attempts to send a message to the actor's mailbox without suspending.</summary>
    public bool TrySend(T message) => _channel.Writer.TryWrite(message);

    /// <summary>Closes the actor's mailbox so it drains remaining messages and finishes gracefully.</summary>
    public bool Close(Exception? error = null) => _channel.Writer.TryComplete(error);

    /// <summary>Immediately cancels the actor coroutine and closes its mailbox.</summary>
    public void Cancel()
    {
        ActorJob.Cancel();
        _channel.Writer.TryComplete(new OperationCanceledException("ActorChannel was canceled."));
    }

    /// <summary>Awaits completion of the actor coroutine.</summary>
    public Task JoinAsync() => ActorJob.JoinAsync();

    /// <summary>Allows awaiting the actor's completion directly.</summary>
    public TaskAwaiter GetAwaiter() => ActorJob.GetAwaiter();
}

/// <summary>
/// Kotlin-style <c>produce</c> and <c>actor</c> structured channel builders on <see cref="CoroutineScope"/>.
/// </summary>
public static class ChannelBuilderExtensions
{
    /// <summary>
    /// Launches a structured producer coroutine that emits elements into a <see cref="ProducedChannel{T}"/>
    /// and automatically closes the channel when the producer block completes or throws.
    /// </summary>
    public static ProducedChannel<T> Produce<T>(
        this CoroutineScope scope,
        Func<ProducerScope<T>, Task> block,
        int capacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend,
        Action<T>? onUndeliveredElement = null,
        TaskScheduler? scheduler = null)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(block);

        var channel = CreateChannel<T>(capacity, onBufferOverflow, onUndeliveredElement, singleWriter: true, singleReader: false);

        var job = scope.Launch(async ct =>
        {
            try
            {
                await Coroutine.Scope(async innerScope =>
                {
                    var producerScope = new ProducerScope<T>(innerScope, channel.Writer);
                    await block(producerScope).ConfigureAwait(false);
                }, ct).ConfigureAwait(false);

                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }, scheduler);

        return new ProducedChannel<T>(channel, job);
    }

    /// <summary>
    /// Launches a structured actor coroutine with an incoming mailbox <see cref="ActorChannel{T}"/>.
    /// Call <see cref="ActorChannel{T}.Close"/> to signal completion after draining queued messages.
    /// </summary>
    public static ActorChannel<T> Actor<T>(
        this CoroutineScope scope,
        Func<ActorScope<T>, Task> block,
        int capacity = 0,
        BufferOverflow onBufferOverflow = BufferOverflow.Suspend,
        Action<T>? onUndeliveredElement = null,
        TaskScheduler? scheduler = null)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(block);

        var channel = CreateChannel<T>(capacity, onBufferOverflow, onUndeliveredElement, singleWriter: false, singleReader: true);

        var job = scope.Launch(async ct =>
        {
            try
            {
                await Coroutine.Scope(async innerScope =>
                {
                    var actorScope = new ActorScope<T>(innerScope, channel.Reader);
                    await block(actorScope).ConfigureAwait(false);
                }, ct).ConfigureAwait(false);

                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }, scheduler);

        return new ActorChannel<T>(channel, job);
    }

    private static Channel<T> CreateChannel<T>(
        int capacity,
        BufferOverflow onBufferOverflow,
        Action<T>? onUndeliveredElement,
        bool singleWriter,
        bool singleReader)
    {
        if (capacity == Channel.Unbounded || capacity < 0)
        {
            return Channel.CreateUnbounded<T>(new UnboundedChannelOptions
            {
                SingleWriter = singleWriter,
                SingleReader = singleReader
            });
        }

        var effectiveCapacity = Math.Max(1, capacity);
        var fullMode = onBufferOverflow switch
        {
            BufferOverflow.Suspend => BoundedChannelFullMode.Wait,
            BufferOverflow.DropOldest => BoundedChannelFullMode.DropOldest,
            BufferOverflow.DropLatest => BoundedChannelFullMode.DropWrite,
            _ => throw new ArgumentOutOfRangeException(nameof(onBufferOverflow))
        };

        var options = new BoundedChannelOptions(effectiveCapacity)
        {
            FullMode = fullMode,
            SingleWriter = singleWriter,
            SingleReader = singleReader
        };

        return onUndeliveredElement is not null
            ? Channel.CreateBounded(options, onUndeliveredElement)
            : Channel.CreateBounded<T>(options);
    }
}

using System.Threading.Channels;

namespace KotlinStyleCoroutines;

/// <summary>
/// Factory methods for creating <see cref="ChannelResult{T}"/> instances without generic static member warnings.
/// </summary>
public static class ChannelResult
{
    /// <summary>
    /// Creates a successful <see cref="ChannelResult{T}"/> containing <paramref name="value"/>.
    /// </summary>
    public static ChannelResult<T> Success<T>(T value) => new(value, isSuccess: true, isClosed: false, exception: null);

    /// <summary>
    /// Creates a closed <see cref="ChannelResult{T}"/> with an optional failure <paramref name="cause"/>.
    /// </summary>
    public static ChannelResult<T> Closed<T>(Exception? cause = null) => new(default!, isSuccess: false, isClosed: true, exception: cause);
}

/// <summary>
/// Represents the outcome of a non-throwing channel receive operation (<c>OnReceiveCatching</c>).
/// </summary>
/// <typeparam name="T">The type of element carried by the channel.</typeparam>
public readonly struct ChannelResult<T> : IEquatable<ChannelResult<T>>
{
    private readonly T _value;

    internal ChannelResult(T value, bool isSuccess, bool isClosed, Exception? exception)
    {
        _value = value;
        IsSuccess = isSuccess;
        IsClosed = isClosed;
        ExceptionOrNull = exception;
    }

    /// <summary>
    /// Gets a value indicating whether an element was successfully received.
    /// </summary>
    public bool IsSuccess { get; }

    /// <summary>
    /// Gets a value indicating whether the channel was closed before an element could be read.
    /// </summary>
    public bool IsClosed { get; }

    /// <summary>
    /// Gets the exception that closed the channel, or <see langword="null"/> if closed normally or succeeded.
    /// </summary>
    public Exception? ExceptionOrNull { get; }

    /// <summary>
    /// Returns the received value, or <see langword="default"/> if the channel was closed.
    /// </summary>
    public T? GetOrNull() => IsSuccess ? _value : default;

    /// <summary>
    /// Returns the received value, or throws <see cref="ExceptionOrNull"/> (or <see cref="ChannelClosedException"/>) if closed.
    /// </summary>
    public T GetOrThrow()
    {
        if (IsSuccess)
            return _value;

        throw ExceptionOrNull ?? new ChannelClosedException();
    }

    /// <inheritdoc />
    public bool Equals(ChannelResult<T> other)
        => IsSuccess == other.IsSuccess
           && IsClosed == other.IsClosed
           && EqualityComparer<T>.Default.Equals(_value, other._value)
           && Equals(ExceptionOrNull, other.ExceptionOrNull);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is ChannelResult<T> other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(IsSuccess, IsClosed, _value, ExceptionOrNull);

    /// <summary>
    /// Determines whether two <see cref="ChannelResult{T}"/> values are equal.
    /// </summary>
    public static bool operator ==(ChannelResult<T> left, ChannelResult<T> right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="ChannelResult{T}"/> values are not equal.
    /// </summary>
    public static bool operator !=(ChannelResult<T> left, ChannelResult<T> right) => !left.Equals(right);
}

/// <summary>
/// Builder used inside <see cref="Coroutine.Select{TResult}"/> to register competing suspending clauses.
/// Exactly one winning clause is selected and its handler executed.
/// </summary>
/// <typeparam name="TResult">The result type returned by the winning clause.</typeparam>
public sealed class SelectBuilder<TResult>
{
    private readonly List<Func<CancellationToken, Func<bool>, Task<Func<Task<TResult>>?>>> _clauses = [];

    internal SelectBuilder()
    {
    }

    internal IReadOnlyList<Func<CancellationToken, Func<bool>, Task<Func<Task<TResult>>?>>> Clauses => _clauses;

    /// <summary>
    /// Registers a clause that wins when an element is read from <paramref name="reader"/>.
    /// </summary>
    public void OnReceive<T>(ChannelReader<T> reader, Func<T, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(handler);

        _clauses.Add(async (ct, tryWin) =>
        {
            while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                if (!tryWin())
                    return null;

                if (reader.TryRead(out var item))
                {
                    return () => handler(item);
                }

                // Another consumer drained the item first; this clause cannot un-claim,
                // so it throws ChannelClosedException only if the channel closed without an item.
                if (reader.Completion.IsCompleted)
                    throw new ChannelClosedException();
            }

            throw new ChannelClosedException();
        });
    }

    /// <summary>
    /// Registers a synchronous-handler clause that wins when an element is read from <paramref name="reader"/>.
    /// </summary>
    public void OnReceive<T>(ChannelReader<T> reader, Func<T, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnReceive(reader, item => Task.FromResult(handler(item)));
    }

    /// <summary>
    /// Registers a clause that wins when either an element is read from <paramref name="reader"/> or <paramref name="reader"/> closes.
    /// </summary>
    public void OnReceiveCatching<T>(ChannelReader<T> reader, Func<ChannelResult<T>, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(handler);

        _clauses.Add(async (ct, tryWin) =>
        {
            while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                if (!tryWin())
                    return null;

                if (reader.TryRead(out var item))
                    return () => handler(ChannelResult.Success(item));
            }

            if (!tryWin())
                return null;

            var closeError = reader.Completion.IsFaulted
                ? reader.Completion.Exception?.InnerException ?? reader.Completion.Exception
                : null;

            return () => handler(ChannelResult.Closed<T>(closeError));
        });
    }

    /// <summary>
    /// Registers a synchronous-handler clause that wins when either an element is read from <paramref name="reader"/> or <paramref name="reader"/> closes.
    /// </summary>
    public void OnReceiveCatching<T>(ChannelReader<T> reader, Func<ChannelResult<T>, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnReceiveCatching(reader, res => Task.FromResult(handler(res)));
    }

    /// <summary>
    /// Registers a clause that wins when <paramref name="deferred"/> completes.
    /// </summary>
    public void OnAwait<T>(Deferred<T> deferred, Func<T, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(deferred);
        ArgumentNullException.ThrowIfNull(handler);

        _clauses.Add(async (ct, tryWin) =>
        {
            var value = await deferred.AwaitAsync().WaitAsync(ct).ConfigureAwait(false);
            return tryWin() ? () => handler(value) : null;
        });
    }

    /// <summary>
    /// Registers a synchronous-handler clause that wins when <paramref name="deferred"/> completes.
    /// </summary>
    public void OnAwait<T>(Deferred<T> deferred, Func<T, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnAwait(deferred, val => Task.FromResult(handler(val)));
    }

    /// <summary>
    /// Registers a clause that wins when <paramref name="task"/> completes.
    /// </summary>
    public void OnAwait<T>(Task<T> task, Func<T, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(task);
        ArgumentNullException.ThrowIfNull(handler);

        _clauses.Add(async (ct, tryWin) =>
        {
            var value = await task.WaitAsync(ct).ConfigureAwait(false);
            return tryWin() ? () => handler(value) : null;
        });
    }

    /// <summary>
    /// Registers a synchronous-handler clause that wins when <paramref name="task"/> completes.
    /// </summary>
    public void OnAwait<T>(Task<T> task, Func<T, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnAwait(task, val => Task.FromResult(handler(val)));
    }

    /// <summary>
    /// Registers a clause that wins when <paramref name="job"/> finishes.
    /// </summary>
    public void OnJoin(Job job, Func<Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(handler);

        _clauses.Add(async (ct, tryWin) =>
        {
            await job.JoinAsync().WaitAsync(ct).ConfigureAwait(false);
            return tryWin() ? handler : null;
        });
    }

    /// <summary>
    /// Registers a synchronous-handler clause that wins when <paramref name="job"/> finishes.
    /// </summary>
    public void OnJoin(Job job, Func<TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnJoin(job, () => Task.FromResult(handler()));
    }

    /// <summary>
    /// Registers a timeout clause that wins if no other clause completes within <paramref name="timeout"/>.
    /// </summary>
    public void OnTimeout(TimeSpan timeout, Func<Task<TResult>> handler, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (timeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout cannot be negative.");

        var tp = timeProvider ?? TimeProvider.System;
        _clauses.Add(async (ct, tryWin) =>
        {
            await Task.Delay(timeout, tp, ct).ConfigureAwait(false);
            return tryWin() ? handler : null;
        });
    }

    /// <summary>
    /// Registers a synchronous-handler timeout clause that wins if no other clause completes within <paramref name="timeout"/>.
    /// </summary>
    public void OnTimeout(TimeSpan timeout, Func<TResult> handler, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnTimeout(timeout, () => Task.FromResult(handler()), timeProvider);
    }
}

public static partial class Coroutine
{
    /// <summary>
    /// Awaits the result of multiple suspending clauses simultaneously, selecting the first clause
    /// that becomes ready, canceling all losing clauses, and executing the winning handler.
    /// </summary>
    public static async Task<TResult> Select<TResult>(
        Action<SelectBuilder<TResult>> configure,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new SelectBuilder<TResult>();
        configure(builder);

        if (builder.Clauses.Count == 0)
            throw new ArgumentException("Select requires at least one registered clause.", nameof(configure));

        var resolvedToken = ResolveToken(cancellationToken);
        using var raceCts = CancellationTokenSource.CreateLinkedTokenSource(resolvedToken);

        var won = 0;
        bool TryWin() => Interlocked.CompareExchange(ref won, 1, 0) == 0;

        var tasks = new List<Task<Func<Task<TResult>>?>>(builder.Clauses.Count);
        foreach (var clause in builder.Clauses)
        {
            tasks.Add(clause(raceCts.Token, TryWin));
        }

        while (tasks.Count > 0)
        {
            var finished = await Task.WhenAny(tasks).ConfigureAwait(false);
            tasks.Remove(finished);

            Func<Task<TResult>>? winnerAction;
            try
            {
                winnerAction = await finished.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (raceCts.IsCancellationRequested && !resolvedToken.IsCancellationRequested)
            {
                continue;
            }
            catch when (Volatile.Read(ref won) == 1)
            {
                // Ignore losing clause exceptions once another clause has already won
                continue;
            }

            if (winnerAction is not null)
            {
                await raceCts.CancelAsync().ConfigureAwait(false);
                return await winnerAction().ConfigureAwait(false);
            }
        }

        resolvedToken.ThrowIfCancellationRequested();
        throw new InvalidOperationException("No select clause succeeded.");
    }
}

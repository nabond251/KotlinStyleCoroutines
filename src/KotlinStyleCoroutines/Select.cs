using System.Threading.Channels;

namespace KotlinStyleCoroutines;

/// <summary>
/// Represents the outcome of <see cref="SelectBuilder{TResult}.OnReceiveCatching{T}"/>:
/// either a received value or a closed channel state.
/// </summary>
public readonly record struct ChannelResult<T>(T? Value, bool IsClosed, Exception? Exception)
{
    /// <summary>Gets whether an element was successfully received.</summary>
    public bool IsSuccess => !IsClosed;

    /// <summary>Creates a successful channel result.</summary>
    public static ChannelResult<T> Success(T value) => new(value, IsClosed: false, Exception: null);

    /// <summary>Creates a closed channel result with an optional failure exception.</summary>
    public static ChannelResult<T> Closed(Exception? exception = null) => new(default, IsClosed: true, Exception: exception);
}

/// <summary>
/// Builder for Kotlin-style <c>select { ... }</c> expressions that race multiple suspendable
/// channel, <see cref="Deferred{T}"/>, <see cref="Job"/>, and timeout clauses and execute at most one winner.
/// </summary>
public sealed class SelectBuilder<TResult>
{
    private readonly Lock _gate = new();
    private readonly List<Func<CancellationToken, CancellationTokenSource, Task>> _arms = [];
    private Func<CancellationToken, Task<TResult>>? _winningHandler;

    internal SelectBuilder() { }

    /// <summary>Registers a clause that wins when an element is read from <paramref name="reader"/>.</summary>
    public void OnReceive<T>(ChannelReader<T> reader, Func<T, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(handler);

        _arms.Add(async (raceCt, raceCts) =>
        {
            while (!raceCt.IsCancellationRequested)
            {
                if (!await reader.WaitToReadAsync(raceCt).ConfigureAwait(false))
                {
                    await reader.Completion.ConfigureAwait(false);
                    throw new ChannelClosedException();
                }

                lock (_gate)
                {
                    if (_winningHandler is not null)
                        return;

                    if (reader.TryRead(out var item))
                    {
                        _winningHandler = ct => handler(item);
                        raceCts.Cancel();
                        return;
                    }
                }
            }
        });
    }

    /// <summary>Registers a synchronous clause that wins when an element is read from <paramref name="reader"/>.</summary>
    public void OnReceive<T>(ChannelReader<T> reader, Func<T, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnReceive(reader, item => Task.FromResult(handler(item)));
    }

    /// <summary>Registers a clause that wins when an element is read from <paramref name="channel"/>.</summary>
    public void OnReceive<T>(ProducedChannel<T> channel, Func<T, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(channel);
        OnReceive(channel.Reader, handler);
    }

    /// <summary>Registers a synchronous clause that wins when an element is read from <paramref name="channel"/>.</summary>
    public void OnReceive<T>(ProducedChannel<T> channel, Func<T, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(channel);
        OnReceive(channel.Reader, handler);
    }

    /// <summary>
    /// Registers a clause that wins either when an element is read from <paramref name="reader"/>
    /// or when <paramref name="reader"/> is closed.
    /// </summary>
    public void OnReceiveCatching<T>(ChannelReader<T> reader, Func<ChannelResult<T>, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(handler);

        _arms.Add(async (raceCt, raceCts) =>
        {
            while (!raceCt.IsCancellationRequested)
            {
                if (!await reader.WaitToReadAsync(raceCt).ConfigureAwait(false))
                {
                    Exception? closeError = null;
                    try
                    {
                        await reader.Completion.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        closeError = ex;
                    }

                    var closedResult = ChannelResult<T>.Closed(closeError);
                    if (TryClaimWinner(ct => handler(closedResult), raceCts))
                        return;

                    return;
                }

                lock (_gate)
                {
                    if (_winningHandler is not null)
                        return;

                    if (reader.TryRead(out var item))
                    {
                        var successResult = ChannelResult<T>.Success(item);
                        _winningHandler = ct => handler(successResult);
                        raceCts.Cancel();
                        return;
                    }
                }
            }
        });
    }

    /// <summary>Registers a synchronous <see cref="OnReceiveCatching{T}(ChannelReader{T}, Func{ChannelResult{T}, Task{TResult}})"/> clause.</summary>
    public void OnReceiveCatching<T>(ChannelReader<T> reader, Func<ChannelResult<T>, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnReceiveCatching(reader, res => Task.FromResult(handler(res)));
    }

    /// <summary>Registers a clause that wins when <paramref name="deferred"/> completes with a value.</summary>
    public void OnAwait<T>(Deferred<T> deferred, Func<T, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(deferred);
        OnAwait(deferred.Task, handler);
    }

    /// <summary>Registers a synchronous clause that wins when <paramref name="deferred"/> completes with a value.</summary>
    public void OnAwait<T>(Deferred<T> deferred, Func<T, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(deferred);
        OnAwait(deferred.Task, handler);
    }

    /// <summary>Registers a clause that wins when <paramref name="task"/> completes with a value.</summary>
    public void OnAwait<T>(Task<T> task, Func<T, Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(task);
        ArgumentNullException.ThrowIfNull(handler);

        _arms.Add(async (raceCt, raceCts) =>
        {
            var value = await task.WaitAsync(raceCt).ConfigureAwait(false);
            TryClaimWinner(ct => handler(value), raceCts);
        });
    }

    /// <summary>Registers a synchronous clause that wins when <paramref name="task"/> completes with a value.</summary>
    public void OnAwait<T>(Task<T> task, Func<T, TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnAwait(task, value => Task.FromResult(handler(value)));
    }

    /// <summary>Registers a clause that wins when <paramref name="job"/> completes.</summary>
    public void OnJoin(Job job, Func<Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(job);
        OnJoin(job.JoinAsync(), handler);
    }

    /// <summary>Registers a synchronous clause that wins when <paramref name="job"/> completes.</summary>
    public void OnJoin(Job job, Func<TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(job);
        OnJoin(job.JoinAsync(), handler);
    }

    /// <summary>Registers a clause that wins when <paramref name="task"/> completes.</summary>
    public void OnJoin(Task task, Func<Task<TResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(task);
        ArgumentNullException.ThrowIfNull(handler);

        _arms.Add(async (raceCt, raceCts) =>
        {
            await task.WaitAsync(raceCt).ConfigureAwait(false);
            TryClaimWinner(_ => handler(), raceCts);
        });
    }

    /// <summary>Registers a synchronous clause that wins when <paramref name="task"/> completes.</summary>
    public void OnJoin(Task task, Func<TResult> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnJoin(task, () => Task.FromResult(handler()));
    }

    /// <summary>Registers a timeout clause that wins if no other clause completes within <paramref name="timeout"/>.</summary>
    public void OnTimeout(TimeSpan timeout, Func<Task<TResult>> handler, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var tp = timeProvider ?? TimeProvider.System;

        _arms.Add(async (raceCt, raceCts) =>
        {
            if (timeout > TimeSpan.Zero)
            {
                await Task.Delay(timeout, tp, raceCt).ConfigureAwait(false);
            }

            TryClaimWinner(_ => handler(), raceCts);
        });
    }

    /// <summary>Registers a synchronous timeout clause.</summary>
    public void OnTimeout(TimeSpan timeout, Func<TResult> handler, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(handler);
        OnTimeout(timeout, () => Task.FromResult(handler()), timeProvider);
    }

    private bool TryClaimWinner(Func<CancellationToken, Task<TResult>> handler, CancellationTokenSource raceCts)
    {
        lock (_gate)
        {
            if (_winningHandler is not null)
                return false;

            _winningHandler = handler;
            raceCts.Cancel();
            return true;
        }
    }

    internal async Task<TResult> ExecuteAsync(CancellationToken ct)
    {
        if (_arms.Count == 0)
            throw new InvalidOperationException("Select expression must register at least one clause.");

        ct.ThrowIfCancellationRequested();

        using var raceCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var tasks = new Task[_arms.Count];

        for (var i = 0; i < _arms.Count; i++)
        {
            var arm = _arms[i];
            tasks[i] = RunArmAsync(arm, raceCts);
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        ct.ThrowIfCancellationRequested();

        Func<CancellationToken, Task<TResult>>? winner;
        lock (_gate)
        {
            winner = _winningHandler;
        }

        if (winner is null)
            throw new InvalidOperationException("Select completed without a winning clause.");

        return await winner(ct).ConfigureAwait(false);
    }

    private async Task RunArmAsync(Func<CancellationToken, CancellationTokenSource, Task> arm, CancellationTokenSource raceCts)
    {
        try
        {
            await arm(raceCts.Token, raceCts).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (raceCts.IsCancellationRequested)
        {
            // Expected when another arm wins or the parent token cancels
        }
        catch (Exception ex)
        {
            // Surface an uncontested arm failure as the winning outcome so it propagates cleanly
            TryClaimWinner(_ => Task.FromException<TResult>(ex), raceCts);
        }
    }
}

public static partial class Coroutine
{
    /// <summary>
    /// Races multiple suspendable clauses (channel receives, <see cref="Deferred{T}"/> awaits,
    /// <see cref="Job"/> joins, and timeouts) and executes the handler of the first clause to become ready.
    /// </summary>
    public static Task<TResult> Select<TResult>(
        Action<SelectBuilder<TResult>> configure,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var resolvedToken = ResolveToken(cancellationToken);
        var builder = new SelectBuilder<TResult>();
        configure(builder);
        return builder.ExecuteAsync(resolvedToken);
    }
}

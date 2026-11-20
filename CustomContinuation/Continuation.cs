using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace KotlinStyleCoroutines;

public sealed class Continuation<T> : INotifyCompletion
{
    private Action? _continuation;
    private T? _result;
    private ExceptionDispatchInfo? _exception;
    private int _state; // 0 = Pending, 1 = Completed, 2 = Canceled
    private Action? _onCancel;

    public bool IsCompleted => Volatile.Read(ref _state) != 0;

    // Called by the C# compiler when suspending
    public void OnCompleted(Action continuation)
    {
        // If already completed before OnCompleted was registered, invoke immediately
        if (Interlocked.CompareExchange(ref _continuation, continuation, null) != null)
        {
            continuation();
        }
    }

    // Called by the C# compiler upon resumption
    public T GetResult()
    {
        if (_state == 2)
            throw new OperationCanceledException();

        _exception?.Throw();
        return _result!;
    }

    // --- Kotlin-style Continuation API ---

    public void Resume(T value)
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            return;

        _result = value;
        Complete();
    }

    public void ResumeWithException(Exception exception)
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            return;

        _exception = ExceptionDispatchInfo.Capture(exception);
        Complete();
    }

    public void InvokeOnCancellation(Action onCancel)
    {
        _onCancel = onCancel;
        if (Volatile.Read(ref _state) == 2)
            onCancel();
    }

    internal void Cancel()
    {
        if (Interlocked.CompareExchange(ref _state, 2, 0) != 0)
            return;

        _onCancel?.Invoke();
        Complete();
    }

    private void Complete()
    {
        // Grab the registered compiler continuation, or place a sentinel so OnCompleted runs inline
        var cont = Interlocked.Exchange(ref _continuation, static () => { });
        cont?.Invoke();
    }
}

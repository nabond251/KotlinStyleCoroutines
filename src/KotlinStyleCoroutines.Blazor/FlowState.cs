namespace KotlinStyleCoroutines.Blazor;

/// <summary>
/// A reactive state holder whose <see cref="Value"/> updates automatically as an upstream flow emits
/// and triggers Blazor component re-rendering.
/// </summary>
/// <typeparam name="T">The type of value held by the state wrapper.</typeparam>
public sealed class FlowState<T>
{
    private T _value;

    internal FlowState(T initialValue) => _value = initialValue;

    /// <summary>
    /// Gets the latest value emitted by the underlying flow.
    /// </summary>
    public T Value
    {
        get => _value;
        internal set => _value = value;
    }
}

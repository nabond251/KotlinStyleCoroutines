namespace KotlinStyleCoroutines.Blazor;

/// <summary>
/// Reactive UI state holder analogous to Jetpack Compose's <c>State&lt;T&gt;</c>.
/// Reading <see cref="Value"/> inside Razor markup renders the latest collected value.
/// </summary>
public sealed class FlowState<T>
{
    private T _value;

    internal FlowState(T initialValue) => _value = initialValue;

    /// <summary>
    /// Gets the latest value emitted by the underlying flow or producer.
    /// </summary>
    public T Value
    {
        get => _value;
        internal set => _value = value;
    }
}

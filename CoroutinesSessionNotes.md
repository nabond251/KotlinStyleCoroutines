# C# Coroutine Implementations (.NET 11)

## 1. Kotlin vs. C# `async`/`await` Summary

* **Kotlin Coroutines:** Built-in structured concurrency (`CoroutineScope`), native continuation manipulation (`suspendCancellableCoroutine`), explicit context switching (`withContext`), and cold streams (`Flow`).
* **C# `async`/`await`:** Hot tasks by default, zero-allocation `ValueTask<T>`, and duck-typed custom awaiters (`GetAwaiter()` / `[AsyncMethodBuilder]`).

## 2. Single-Shot Custom Continuation (`suspendCancellableCoroutine`)

Replicates Kotlin's `suspendCancellableCoroutine` using a custom `INotifyCompletion` awaiter (`Continuation<T>`) and `Suspendable<T>` struct wrapper, resuming inline without `TaskCompletionSource` overhead.

## 3. Lua-Style Multi-Shot Coroutines (`LuaCoroutine<TIn, TOut>`)

Uses a custom `[AsyncMethodBuilder(typeof(LuaCoroutineMethodBuilder<,>))]` and `AsyncLocal` context (`Lua.Yield<TIn, TOut>(value)`) to support two-way yielding and resuming across multiple steps, including single-boxing of the compiler-generated state machine for Release-build safety in .NET 11.

### Usage Example

```csharp
using LuaCoroutines = KotlinStyleCoroutines;

static async LuaCoroutines.LuaCoroutine<string, int> DialogueNode()
{
    string firstChoice = await LuaCoroutines.Lua.Yield<string, int>(100);
    Console.WriteLine($"Coroutine received: {firstChoice}");

    string secondChoice = await LuaCoroutines.Lua.Yield<string, int>(200);
    Console.WriteLine($"Coroutine received: {secondChoice}");

    return 999;
}

var co = DialogueNode();
var step1 = co.Resume("Start");      // (True, 100)
var step2 = co.Resume("Open Door");  // (True, 200)
var step3 = co.Resume("Attack");     // (True, 999), co.IsDead == true
```

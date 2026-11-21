// See https://kotlinlang.org/docs/coroutines-basics.html

using KotlinStyleCoroutines;

await Coroutine.Scope(async scope =>
{
    _ = scope.Launch(token =>
    {
        scope.Launch(async token =>
        {
            await Task.Delay(2000, token);
            Console.WriteLine("Child of the enclosing coroutine completed");
        });

        Console.WriteLine("Child coroutine 1 completed");
        return Task.CompletedTask;
    });

    _ = scope.Launch(async token =>
    {
        await Task.Delay(1000, token);
        Console.WriteLine("Child coroutine 2 completed");
    });
});

// Runs only after all children in the coroutineScope have completed
Console.WriteLine("Coroutine scope completed");

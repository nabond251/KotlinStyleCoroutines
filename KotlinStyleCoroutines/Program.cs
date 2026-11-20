using KotlinStyleCoroutines;

await Coroutine.WithContext(Dispatchers.Default, async scope =>
{
    scope.Launch(token => Greet());

    scope.Launch(async token =>
    {
        Console.WriteLine($"The CoroutineScope.Launch() on the thread: {Thread.CurrentThread.Name}");
        await Task.Delay(1000);
    });

    Console.WriteLine($"The WithContext() on the thread: {Thread.CurrentThread.Name}");
});

async Task Greet()
{
    Console.WriteLine($"The Greet() on the thread: {Thread.CurrentThread.Name}");
    await Task.Delay(1000);
}

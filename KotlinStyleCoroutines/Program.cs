using KotlinStyleCoroutines;

await Coroutine.WithContext(Dispatchers.Default, async token =>
{
    await Greet();
    Console.WriteLine($"The WithContext() on the thread: {Thread.CurrentThread.Name}");
});

async Task Greet()
{
    Console.WriteLine($"The Greet() on the thread: {Thread.CurrentThread.Name}");
    await Task.Delay(1000);
}

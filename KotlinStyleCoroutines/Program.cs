using KotlinStyleCoroutines;

await ShowUserInfo();

async Task ShowUserInfo()
{
    Console.WriteLine("Loading user...");
    await Greet();
    Console.WriteLine("User: John Smith");
}

async Task Greet()
{
    Console.WriteLine("Hello world from a suspending function");
}

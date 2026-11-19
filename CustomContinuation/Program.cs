async Task<string> FetchDataAsync(CancellationToken ct)
{
    return await Coroutine.SuspendCancellable<string>(cont =>
    {
        var request = new LegacyCallbackRequest();

        request.Completed += data => cont.Resume(data);
        request.Failed += ex => cont.ResumeWithException(ex);

        cont.InvokeOnCancellation(() => request.Abort());
        request.Start();
    }, ct);
}

Console.WriteLine(await FetchDataAsync(CancellationToken.None));

// Define a coroutine that takes string commands in and yields int status codes out
static async LuaCoroutine<string, int> DialogueNode(CoroutineContext<string, int> ctx)
{
    // Suspends immediately upon creation; receives input from first Resume()
    string firstChoice = await ctx.Yield(100);
    Console.WriteLine($"Coroutine received: {firstChoice}");

    string secondChoice = await ctx.Yield(200);
    Console.WriteLine($"Coroutine received: {secondChoice}");

    // Final return marks IsDead = true
    return 999;
}

// --- Driving the coroutine ---
var ctx = new CoroutineContext<string, int>();
var co = DialogueNode(ctx); // Created in "suspended" state (nothing printed yet)

var step1 = co.Resume("IgnorableInitialInput"); // Runs until first Yield(100)
Console.WriteLine($"Caller got: {step1.Value}, Dead: {co.IsDead}"); // 100, False

var step2 = co.Resume("Open Door");             // Resumes with "Open Door", runs to Yield(200)
Console.WriteLine($"Caller got: {step2.Value}, Dead: {co.IsDead}"); // 200, False

var step3 = co.Resume("Attack");                // Resumes with "Attack", hits return 999
Console.WriteLine($"Caller got: {step3.Value}, Dead: {co.IsDead}"); // 999, True

var step4 = co.Resume("Anything");              // Dead coroutine returns (false, 0)
Console.WriteLine($"Success: {step4.Success}");                     // False

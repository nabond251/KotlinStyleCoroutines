using KotlinStyleCoroutines;

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

async Task<string> LoadDashboardAsync(CancellationToken ct)
{
    return await Coroutine.Scope(async scope =>
    {
        // Start two concurrent operations
        Task<string> userTask = scope.Async(token => FetchDataAsync(token));
        Task<string> postsTask = scope.Async(token => FetchDataAsync(token));

        // Await results; even if we return early or throw here, DisposeAsync awaits all children
        var user = await userTask;
        var posts = await postsTask;

        return user + posts;
    }, ct);
}

Console.WriteLine(await LoadDashboardAsync(CancellationToken.None));

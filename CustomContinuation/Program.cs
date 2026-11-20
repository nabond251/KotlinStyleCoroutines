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

internal class LegacyCallbackRequest
{
    public LegacyCallbackRequest()
    {
    }

    public event Action<string>? Completed;

    public event Action<Exception>? Failed;

    internal void Abort()
    {
        throw new NotImplementedException();
    }

    internal void Start()
    {
        Task.Run(async () =>
        {
            await Task.Delay(1000);
            Completed?.Invoke("data");
        });
    }
}
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using KotlinStyleCoroutines;
using Xunit;

namespace KotlinStyleCoroutines.Tests;

public class FlowOperatorsTests
{
    private static readonly string[] ExpectedZipped = ["A1+B1", "A2+B2"];
    private static readonly string[] ExpectedFlatMapLatest = ["result:kotlin"];
    private static readonly string[] ExpectedDebounced = ["kot", "kotlin"];
    private static readonly string[] ExpectedRecovered =
    [
        "attempt-1-part1",
        "attempt-2-part1",
        "attempt-3-part1",
        "attempt-3-part2"
    ];
    private static readonly string[] ExpectedFallback = ["before-error", "before-error", "recovered:Fatal"];

    [Fact]
    public async Task CombineAndMergeAndZipComposeFlowsCleanly()
    {
        var user = new MutableStateFlow<string>("Alice");
        var role = new MutableStateFlow<string>("Viewer");

        var combinedLabels = new List<string>();
        using var combineCts = new CancellationTokenSource();

        await Coroutine.Scope(async scope =>
        {
            var job = user.CombineWith(role, (u, r) => $"{u}:{r}")
                .LaunchIn(scope, label =>
                {
                    combinedLabels.Add(label);
                    if (label == "Bob:Admin")
                        combineCts.Cancel();
                });

            while (user.SubscriptionCount.Value == 0 || role.SubscriptionCount.Value == 0)
                await Task.Delay(10);

            await Task.Delay(20);
            role.Value = "Admin";
            await Task.Delay(30);
            user.Value = "Bob";

            await job.JoinAsync();
        }, combineCts.Token);

        Assert.Contains("Alice:Viewer", combinedLabels);
        Assert.Contains("Alice:Admin", combinedLabels);
        Assert.Equal("Bob:Admin", combinedLabels[^1]);

        static async IAsyncEnumerable<string> StreamA()
        {
            yield return "A1";
            await Task.Delay(15);
            yield return "A2";
        }

        static async IAsyncEnumerable<string> StreamB()
        {
            yield return "B1";
            await Task.Delay(15);
            yield return "B2";
        }

        var merged = new List<string>();
        await foreach (var item in Coroutine.Merge(StreamA(), StreamB()))
        {
            merged.Add(item);
        }
        Assert.Equal(4, merged.Count);
        Assert.Contains("A1", merged);
        Assert.Contains("B2", merged);

        var zipped = new List<string>();
        await foreach (var pair in StreamA().ZipWith(StreamB(), (a, b) => $"{a}+{b}"))
        {
            zipped.Add(pair);
        }
        Assert.Equal(ExpectedZipped, zipped);
    }

    [Fact]
    public async Task FlatMapLatestCancelsPreviousInFlightSearchWhenNewQueryArrives()
    {
        int canceledSearches = 0;

        static async IAsyncEnumerable<string> Queries()
        {
            yield return "kot";
            await Task.Delay(20);
            yield return "kotlin";
        }

        async IAsyncEnumerable<string> SearchApi(
            string query,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                await Task.Delay(60, ct);
                yield return $"result:{query}";
            }
            finally
            {
                if (ct.IsCancellationRequested)
                    Interlocked.Increment(ref canceledSearches);
            }
        }

        var results = new List<string>();
        await foreach (var item in Queries().FlatMapLatest(q => SearchApi(q)))
        {
            results.Add(item);
        }

        Assert.Equal(1, canceledSearches);
        Assert.Equal(ExpectedFlatMapLatest, results);
    }

    [Fact]
    public async Task DebounceAndSampleFilterRapidEmissions()
    {
        static async IAsyncEnumerable<string> RapidTyping()
        {
            yield return "k";
            await Task.Delay(15);
            yield return "ko";
            await Task.Delay(15);
            yield return "kot";
            await Task.Delay(80);
            yield return "kotl";
            await Task.Delay(15);
            yield return "kotlin";
        }

        var debounced = new List<string>();
        await foreach (var word in RapidTyping().Debounce(TimeSpan.FromMilliseconds(50)))
        {
            debounced.Add(word);
        }
        Assert.Equal(ExpectedDebounced, debounced);

        static async IAsyncEnumerable<int> HighFrequencyCounter()
        {
            for (int i = 1; i <= 6; i++)
            {
                yield return i;
                await Task.Delay(20);
            }
        }

        var sampled = new List<int>();
        await foreach (var tick in HighFrequencyCounter().Sample(TimeSpan.FromMilliseconds(45)))
        {
            sampled.Add(tick);
        }
        Assert.NotEmpty(sampled);
        Assert.True(sampled.Count < 6);
    }

    [Fact]
    public async Task RetryWhenAndCatchRecoverFromTransientFailures()
    {
        int attempts = 0;

        async IAsyncEnumerable<string> FlakyStream()
        {
            attempts++;
            yield return $"attempt-{attempts}-part1";
            if (attempts < 3)
                throw new TimeoutException("Transient network drop");

            yield return $"attempt-{attempts}-part2";
        }

        var recovered = new List<string>();
        await foreach (var item in FlakyStream()
            .Retry(retries: 3, predicate: ex => ValueTask.FromResult(ex is TimeoutException))
            .Catch(ex => $"fallback:{ex.Message}"))
        {
            recovered.Add(item);
        }

        Assert.Equal(3, attempts);
        Assert.Equal(ExpectedRecovered, recovered);

        static async IAsyncEnumerable<string> AlwaysFails()
        {
            yield return "before-error";
            throw new InvalidOperationException("Fatal");
        }

        var withFallback = new List<string>();
        await foreach (var item in AlwaysFails()
            .Retry(retries: 1)
            .Catch(ex => $"recovered:{ex.Message}"))
        {
            withFallback.Add(item);
        }

        Assert.Equal(ExpectedFallback, withFallback);
    }
}

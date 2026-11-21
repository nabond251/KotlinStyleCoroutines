# Contributing to KotlinStyleCoroutines

Thank you for contributing to `KotlinStyleCoroutines`! This guide covers local environment setup, repository layout, coding conventions, and the pull request workflow.

---

## Repository Structure

```text
KotlinStyleCoroutines/
├── .editorconfig                              # C# code style & formatting rules
├── .github/workflows/dotnet.yml               # CI build, test, sample run, & pack workflow
├── Directory.Build.props                      # Shared .NET 11, Nullable, & analyzer settings
├── KotlinStyleCoroutines.slnx                 # Solution file linking all projects
├── docs/                                      # Contributor & architecture documentation
├── samples/
│   ├── BlazorSearchDemo/                      # Interactive Blazor Server + ViewModel demo
│   ├── CoroutineBasics/                       # Nested structured concurrency console sample
│   └── SearchDemo/                            # Console reactive Flow pipeline sample
├── src/
│   ├── KotlinStyleCoroutines/                 # Core concurrency, continuations, & Flow library
│   └── KotlinStyleCoroutines.Blazor/          # Blazor lifecycle bridges (CollectAsStateWithLifecycle)
└── tests/
    └── KotlinStyleCoroutines.Tests/           # xUnit test suite
```

---

## Prerequisites & Local Setup

* **.NET 11 SDK** (`11.0.x` preview or GA)
* Any C# 15-capable IDE (Visual Studio 2026, JetBrains Rider, or VS Code with C# Dev Kit)

### Build, Test, and Run Samples

Run all commands from the repository root:

```bash
# Restore and build all projects in Release mode (warnings are treated as errors)
dotnet build KotlinStyleCoroutines.slnx --configuration Release

# Run the xUnit test suite
dotnet test KotlinStyleCoroutines.slnx --configuration Release --no-build --verbosity normal

# Run the console samples
dotnet run --project samples/CoroutineBasics/CoroutineBasics.csproj --configuration Release --no-build
dotnet run --project samples/SearchDemo/SearchDemo.csproj --configuration Release --no-build
```

---

## Coding Standards & Analyzer Rules

`Directory.Build.props` enforces strict build quality across all projects in the solution:

* **`<TreatWarningsAsErrors>true</TreatWarningsAsErrors>`**
* **`<AnalysisLevel>latest-recommended</AnalysisLevel>`**
* **`<EnforceCodeStyleInBuild>true</EnforceCodeStyleInBuild>`**
* **`<Nullable>enable</Nullable>`** and **`<ImplicitUsings>enable</ImplicitUsings>`**

When adding or modifying code, adhere to these conventions:

1. **File-Scoped Namespaces & Collection Expressions:** Follow `.editorconfig` rules (`namespace KotlinStyleCoroutines;`, `[...]` collection expressions, and target-typed `new()`).
2. **Handle `Job` Return Values Explicitly:** Because `Job` implements `GetAwaiter()`, Roslyn emits **CS4014** if `scope.Launch(...)` is called without awaiting or assigning the result. Either store the `Job` or explicitly discard it with `_ = scope.Launch(...)`.
3. **Test Naming (CA1707):** Do not use underscores in public test method names (e.g., use `CombineAndMergeAndZipComposeFlowsCleanly`, not `Combine_And_Merge...`).
4. **Static Readonly Arrays in Tests (CA1861):** Extract repeated constant array assertions into `private static readonly` fields.
5. **Virtual Time Over Wall-Clock Delays:** Any new time-based operator must accept an optional `TimeProvider? timeProvider = null` parameter (defaulting to `TimeProvider.System`) so tests can run deterministically without flaky `Task.Delay` sleeps.

---

## Pull Request Workflow

1. **Create a focused branch** from `master` (e.g., `feat/distinct-until-changed` or `fix/channel-flow-disposal`).
2. **Add unit tests** in `tests/KotlinStyleCoroutines.Tests/` covering normal behavior, cancellation propagation, and error handling.
3. **Verify zero warnings and passing tests** locally with `dotnet test KotlinStyleCoroutines.slnx --configuration Release`.
4. **Update documentation** in `README.md` or `docs/` if you add or modify public APIs.
5. **Open a Pull Request** describing the motivation, design trade-offs, and test coverage.

using System.Collections.ObjectModel;

namespace BasicCodelab.Models;

public sealed record WellnessUiState(ReadOnlyCollection<WellnessTask> Tasks);

using System.Collections.ObjectModel;
using KotlinStyleCoroutines;

namespace BasicCodelab.Models;

public sealed class WellnessViewModel
{
    private readonly MutableStateFlow<WellnessUiState> _uiState = new(CreateInitialState());

    public IStateFlow<WellnessUiState> UiState => _uiState.AsStateFlow();

    public void Remove(WellnessTask item)
    {
        ArgumentNullException.ThrowIfNull(item);

        _uiState.UpdateAndGet(state =>
        {
            var updatedTasks = state.Tasks.Where(task => task.Id != item.Id).ToArray();
            return updatedTasks.Length == state.Tasks.Count
                ? state
                : state with { Tasks = Array.AsReadOnly(updatedTasks) };
        });
    }

    public void ChangeTaskChecked(WellnessTask item, bool isChecked)
    {
        ArgumentNullException.ThrowIfNull(item);

        _uiState.UpdateAndGet(state =>
        {
            var index = -1;
            for (var i = 0; i < state.Tasks.Count; i++)
            {
                if (state.Tasks[i].Id == item.Id)
                {
                    index = i;
                    break;
                }
            }

            if (index < 0 || state.Tasks[index].IsChecked == isChecked)
            {
                return state;
            }

            var updatedTasks = state.Tasks.ToArray();
            updatedTasks[index] = updatedTasks[index] with { IsChecked = isChecked };
            return state with { Tasks = Array.AsReadOnly(updatedTasks) };
        });
    }

    private static WellnessUiState CreateInitialState()
    {
        var tasks = Enumerable
            .Range(0, 30)
            .Select(index => new WellnessTask(index, $"Task # {index}"))
            .ToArray();
        return new WellnessUiState(Array.AsReadOnly(tasks));
    }
}

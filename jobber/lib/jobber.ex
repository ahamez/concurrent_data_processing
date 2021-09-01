defmodule Jobber do
  alias Jobber.{JobRunner, JobSupervisor}

  def start_job(args) do
    nb_runnings =
      args
      |> Keyword.get(:type)
      |> running()
      |> Enum.count()

    if nb_runnings >= 5 do
      {:error, :too_much_jobs}
    else
      DynamicSupervisor.start_child(JobRunner, {JobSupervisor, args})
    end
  end

  def running(type) do
    match_all = {:"$1", :"$2", :"$3"}
    filter = [{:==, :"$3", type}]
    map_result = [%{id: :"$1", pid: :"$2", type: :"$3"}]

    Registry.select(Jobber.JobRegistry, [{match_all, filter, map_result}])
  end
end

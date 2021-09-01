good_job = fn ->
  Process.sleep(2_000)

  {:ok, []}
end

long_good_job = fn ->
  Process.sleep(60_000)

  {:ok, []}
end

bad_job = fn ->
  Process.sleep(2_000)

  :error
end

exception_job = fn ->
  Process.sleep(1_000)

  raise :oops_i_did_it_again
end

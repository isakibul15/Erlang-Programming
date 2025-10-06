%%% test_node4.erl — replication survives owner failure
-module(test_node4).
-export([run/0, kill/1]).

run() ->
    timer:start(),
    A = node4:start(key:generate()),
    B = node4:start(key:generate(), A),
    C = node4:start(key:generate(), B),
    timer:sleep(4000),
    node4:probe(A),

    %% choose a key owned by A to make behavior deterministic
    AId = get_id(A),
    K = AId,
    ok = node4:add(A, K, <<"replicated">>),

    %% allow periodic replication to run at least once
    timer:sleep(6000),

    %% verify reachable from C
    {K,<<"replicated">>} = node4:lookup(C, K),

    %% kill A (the owner) and check that C (a successor replica) still serves
    kill(A),
    timer:sleep(4000),
    {K,<<"replicated">>} = node4:lookup(B, K),
    io:format("Replication survived owner failure.~n", []),
    ok.

kill(Pid) -> exit(Pid, kill).

get_id(Pid) ->
    Pid ! {print, self()},
    receive
        {state, Id, _Pred, _Succ, _Next, _Succ2, _Store} ->
            Id
    after 2000 -> exit(timeout)
    end.

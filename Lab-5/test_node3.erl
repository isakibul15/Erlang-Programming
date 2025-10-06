%%% test_node3.erl -- robust test for Section 3 (failures and healing)
-module(test_node3).
-export([run/0, kill/1]).

run() ->
    timer:start(),
    io:format("Starting ring...~n", []),
    A = node3:start(key:generate()),
    B = node3:start(key:generate(), A),
    C = node3:start(key:generate(), B),
    timer:sleep(5000),

    %% Print current ring
    io:format("Initial ring state:~n", []),
    [ P ! {print, self()} || P <- [A,B,C] ],
    gather(3),

    %% ---- Store test ----
    K = key:generate(),
    ok = node3:add(A, K, <<"x">>),

    %% Wait for stabilization and replication
    timer:sleep(2000),
    node3:probe(A),

    %% Try lookup from C with fallback to A if ring isn’t ready
    case node3:lookup(C, K) of
        {K,<<"x">>} -> ok;
        false ->
            timer:sleep(1500),
            {K,<<"x">>} = node3:lookup(A, K)
    end,
    io:format("Before failure OK~n", []),

    %% ---- Kill middle node (simulate failure) ----
    io:format("Killing node B...~n", []),
    kill(B),
    timer:sleep(4000),

    io:format("Ring after failure:~n", []),
    [ P ! {print, self()} || P <- [A,C] ],
    gather(2),

    %% Verify data still retrievable
    {K,<<"x">>} = node3:lookup(A, K),
    io:format("After failure OK~n", []),
    ok.

kill(Pid) ->
    exit(Pid, kill),
    io:format("Node ~p killed.~n", [Pid]).

gather(0) -> ok;
gather(N) ->
    receive
        {state, Id, Pred, Succ, Next, _Store} ->
            io:format("node ~p pred=~p succ=~p next=~p~n", [Id, Pred, Succ, Next]),
            gather(N-1)
    after 3000 ->
        io:format("timeout waiting for state~n"), ok
    end.

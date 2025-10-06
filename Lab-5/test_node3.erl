%%% test_node3.erl -- Section 3 test with deterministic key ownership
-module(test_node3).
-export([run/0, kill/1]).

run() ->
    timer:start(),
    io:format("Starting ring...~n", []),
    A = node3:start(key:generate()),
    B = node3:start(key:generate(), A),
    C = node3:start(key:generate(), B),
    timer:sleep(5000),

    io:format("Initial ring state:~n", []),
    [ P ! {print, self()} || P <- [A,B,C] ],
    gather(3),

    %% pick a key that A owns: (PKey, AId] -> choose K = AId
    {AId, PredA, _SuccA, _NextA} = state_of(A),
    K = AId,
    ok = node3:add(A, K, <<"x">>),

    timer:sleep(1500),
    node3:probe(A),

    %% lookup via any node; allow one retry
    ensure_lookup([C,A], K, <<"x">>),
    io:format("Before failure OK~n", []),

    io:format("Killing node B...~n", []),
    kill(B),
    timer:sleep(4000),

    io:format("Ring after failure:~n", []),
    [ P ! {print, self()} || P <- [A,C] ],
    gather(2),

    %% the key is owned by A, so it must still be present
    ensure_lookup([A,C], K, <<"x">>),
    io:format("After failure OK~n", []),
    ok.

kill(Pid) ->
    exit(Pid, kill),
    io:format("Node ~p killed.~n", [Pid]).

%% --- helpers ---

state_of(Pid) ->
    Pid ! {print, self()},
    receive
        {state, Id, Pred, Succ, Next, _Store} ->
            {Id, Pred, Succ, Next}
    after 3000 ->
            exit(timeout_state)
    end.

ensure_lookup([Pid|Rest], K, V) ->
    case node3:lookup(Pid, K) of
        {K, V} -> ok;
        false when Rest =/= [] ->
            timer:sleep(1000), ensure_lookup(Rest, K, V);
        _ -> exit({lookup_failed, K})
    end.

gather(0) -> ok;
gather(N) ->
    receive
        {state, Id, Pred, Succ, Next, _Store} ->
            io:format("node ~p pred=~p succ=~p next=~p~n", [Id, Pred, Succ, Next]),
            gather(N-1)
    after 3000 ->
        io:format("timeout waiting for state~n"), ok
    end.

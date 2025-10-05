%% Test for Part 2: ring + store
-module(test_node2).
-export([run/0]).

run() ->
    timer:start(),
    A = node2:start(key:generate()),
    timer:sleep(50),
    B = node2:start(key:generate(), A),
    timer:sleep(50),
    C = node2:start(key:generate(), B),
    timer:sleep(3500),

    %% add some KVs via different entry points
    K1 = key:generate(), K2 = key:generate(), K3 = key:generate(),
    ok = node2:add(A, K1, <<"alpha">>),
    ok = node2:add(B, K2, <<"beta">>),
    ok = node2:add(C, K3, <<"gamma">>),

    %% lookups should succeed from any node
    R1 = node2:lookup(A, K1),
    R2 = node2:lookup(B, K2),
    R3 = node2:lookup(C, K3),
    io:format("Lookup results: ~p ~p ~p~n", [R1,R2,R3]),

    %% optional: print state
    lists:foreach(fun(P) -> P ! {print, self()} end, [A,B,C]),
    gather(3),

    ok.

gather(0) -> ok;
gather(N) ->
    receive
        {state, Id, Pred, Succ, _Store} ->
            io:format("node ~p pred=~p succ=~p~n", [Id, Pred, Succ]),
            gather(N-1)
    after 2000 ->
        io:format("timeout waiting for state~n"), ok
    end.

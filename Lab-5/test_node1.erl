
%% Simple test runner for Section 1 ring
-module(test_node1).
-export([run/0]).

run() ->
    crypto:start(),
    timer:start(),
    io:format("Starting node A~n"),
    A = node1:start(key:generate()),
    receive after 50 -> ok end,
    io:format("Starting node B joining A~n"),
    B = node1:start(key:generate(), A),
    receive after 50 -> ok end,
    io:format("Starting node C joining B~n"),
    C = node1:start(key:generate(), B),

    %% wait for a few stabilize ticks
    timer:sleep(3500),

    %% ask each to print state
    A ! {print, self()},
    B ! {print, self()},
    C ! {print, self()},
    States = gather(3, []),
    io:format("States: ~p~n", [States]),
    ok.

gather(0, Acc) -> lists:reverse(Acc);
gather(N, Acc) ->
    receive
        {state, Id, Pred, Succ} ->
            io:format("node ~p pred=~p succ=~p~n", [Id, Pred, Succ]),
            gather(N-1, [{Id, Pred, Succ}|Acc])
    after 2000 ->
        io:format("timeout waiting for state~n"),
        lists:reverse(Acc)
    end.

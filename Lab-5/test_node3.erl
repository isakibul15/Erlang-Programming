-module(test_node3).
-export([run/0, kill/1]).

run() ->
    timer:start(),
    A = node3:start(key:generate()),
    B = node3:start(key:generate(), A),
    C = node3:start(key:generate(), B),
    timer:sleep(3000),
    [ P ! {print, self()} || P <- [A,B,C] ],
    gather(3),
    K = key:generate(),
    ok = node3:add(A, K, <<"x">>),
    {K,<<"x">>} = node3:lookup(C, K),
    io:format("Before failure OK~n", []),
    kill(B),
    timer:sleep(2000),
    [ P ! {print, self()} || P <- [A,C] ],
    gather(2),
    {K,<<"x">>} = node3:lookup(A, K),
    io:format("After failure OK~n", []),
    ok.

kill(Pid) -> exit(Pid, kill).

gather(0) -> ok;
gather(N) ->
    receive
        {state, Id, Pred, Succ, Next, _Store} ->
            io:format("node ~p pred=~p succ=~p next=~p~n", [Id, Pred, Succ, Next]),
            gather(N-1)
    after 2000 -> io:format("timeout waiting for state~n"), ok
    end.

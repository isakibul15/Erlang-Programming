%% Test for Part 2: ring + store (+ probe, handover, throughput)
-module(test_node2).
-export([run/0]).

run() ->
    timer:start(),

    %% --- form ring ---
    A = node2:start(key:generate()),
    timer:sleep(50),
    B = node2:start(key:generate(), A),
    timer:sleep(50),
    C = node2:start(key:generate(), B),
    timer:sleep(3500),

    %% --- basic add/lookup sanity ---
    K1 = key:generate(), K2 = key:generate(), K3 = key:generate(),
    ok = node2:add(A, K1, <<"alpha">>),
    ok = node2:add(B, K2, <<"beta">>),
    ok = node2:add(C, K3, <<"gamma">>),
    R1 = node2:lookup(A, K1),
    R2 = node2:lookup(B, K2),
    R3 = node2:lookup(C, K3),
    io:format("Lookup results: ~p ~p ~p~n", [R1,R2,R3]),
    lists:foreach(fun(P) -> P ! {print, self()} end, [A,B,C]),
    gather(3),

    %% --- probe round-trip ---
    io:format("Launching probe from A~n"),
    node2:probe(A),
    timer:sleep(1200),

    %% --- handover check after join ---
    Ks = [key:generate() || _ <- lists:seq(1,300)],
    lists:foreach(fun(K) -> ok = node2:add(A, K, <<"v">>) end, Ks),
    _D = node2:start(key:generate(), C),
    timer:sleep(3500),
    OkCnt = count_ok_lookups(B, Ks),
    io:format("Post-join lookups ok=~p/~p~n", [OkCnt, length(Ks)]),

    %% --- throughput microbenchmarks ---
    N = 1000,
    AddKeys = [key:generate() || _ <- lists:seq(1,N)],

    T0a = now_ms(),
    lists:foreach(fun(K) -> ok = node2:add(B, K, <<"x">>) end, AddKeys),
    T1a = now_ms(),
    AddQps = qps(N, T0a, T1a),
    io:format("Adds: N=~p elapsed=~p ms qps=~p~n", [N, T1a-T0a, AddQps]),

    T0l = now_ms(),
    _ = [node2:lookup(C, K) || K <- AddKeys],
    T1l = now_ms(),
    LookupQps = qps(N, T0l, T1l),
    io:format("Lookups: N=~p elapsed=~p ms qps=~p~n", [N, T1l-T0l, LookupQps]),

    ok.

%% ----- helpers -----

gather(0) -> ok;
gather(N) ->
    receive
        {state, Id, Pred, Succ, _Store} ->
            io:format("node ~p pred=~p succ=~p~n", [Id, Pred, Succ]),
            gather(N-1)
    after 2000 ->
        io:format("timeout waiting for state~n"), ok
    end.

count_ok_lookups(Pid, Ks) ->
    Replies = [node2:lookup(Pid, K) || K <- Ks],
    length([1 || {_, _} <- Replies]).

now_ms() ->
    erlang:monotonic_time(millisecond).

qps(N, T0, T1) ->
    ElapsedMs = max(1, T1 - T0),
    (N * 1000) / ElapsedMs.

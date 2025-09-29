-module(gms_stress).
-compile(export_all).

%% Public entry points
run_gms2(N, Rounds) -> run(gms2, N, Rounds, 350).
run_gms3(N, Rounds) -> run(gms3, N, Rounds, 350).

%% Start N workers on Module; then for Rounds:
%% - randomly kill 1..K workers (K varies)
%% - add the same number of new workers joining a survivor
%% - occasionally kill multiple in a burst
run(Module, N, Rounds, Sleep) when N >= 3, Rounds >= 1 ->
    rand:seed(exsplus, {101,102,103}),
    io:format("=== ~p stress: N=~p rounds=~p ===~n", [Module, N, Rounds]),
    Workers0 = start_group(Module, N, Sleep),
    loop(Module, Sleep, 1, Rounds, N, Workers0),
    ok.

%% -------- core loop --------
loop(_Module, _Sleep, R, R, _TargetN, Workers) ->
    io:format("Final cleanup (~p workers)~n", [length(Workers)]),
    stop_all(Workers);
loop(Module, Sleep, I, Rounds, TargetN, Workers0) ->
    io:format("Round ~p: workers=~p~n", [I, length(Workers0)]),
    timer:sleep(800),

    %% choose how many to kill this round
    MaxKill = min( max(1, length(Workers0) div 4), 4 ),
    KillCount = 1 + rand:uniform(MaxKill),
    KillList  = pick_unique(Workers0, KillCount),

    lists:foreach(fun(W) -> io:format("KILL ~p~n",[W]), exit(W, kill) end, KillList),
    Survivors = [W || W <- Workers0, not lists:member(W, KillList)],
    timer:sleep(1200),

    %% add same number of replacements, attach to any survivor
    AttachPeer = case Survivors of [] -> undefined; [P|_] -> P end,
    {Workers1, _NextId} =
        add_replacements(Module, Survivors, AttachPeer, Sleep, KillCount, next_id(seed_id())+I*1000),

    %% occasionally burst kill more
    Workers2 =
      case (I rem 3) of
        0 ->
          Extra = min(2, max(0, length(Workers1)-2)),
          ExtraList = pick_unique(Workers1, Extra),
          lists:foreach(fun(W) -> io:format("BURST KILL ~p~n",[W]), exit(W, kill) end, ExtraList),
          timer:sleep(900),
          [W || W <- Workers1, not lists:member(W, ExtraList)];
        _ -> Workers1
      end,

    %% replenish to target N
    Workers3 =
      case length(Workers2) < TargetN of
        true  -> replenish(Module, Workers2, Sleep, TargetN);
        false -> Workers2
      end,

    loop(Module, Sleep, I+1, Rounds, TargetN, Workers3).

%% -------- start helpers --------
start_group(Module, N, Sleep) ->
    W1 = worker:start(1, Module, 111, Sleep),
    timer:sleep(700),
    Ws = start_joins(Module, 2, N, W1, Sleep, []),
    [W1 | Ws].

start_joins(_Module, Id, N, _Peer, _Sleep, Acc) when Id > N ->
    lists:reverse(Acc);
start_joins(Module, Id, N, Peer, Sleep, Acc) ->
    W = worker:start(Id, Module, 100 + Id, Peer, Sleep),
    timer:sleep(300),
    start_joins(Module, Id+1, N, Peer, Sleep, [W|Acc]).

%% -------- add replacements --------
add_replacements(_Module, Workers, undefined, _Sleep, 0, NextId) ->
    {Workers, NextId};
add_replacements(Module, Workers, _Peer, _Sleep, 0, NextId) ->
    {Workers, NextId};
add_replacements(Module, Workers, Peer, Sleep, K, NextId) when K > 0 ->
    W = worker:start(NextId, Module, 100 + NextId, Peer, Sleep),
    timer:sleep(250),
    add_replacements(Module, [W|Workers], Peer, Sleep, K-1, NextId+1).

replenish(_Module, Workers, _Sleep, TargetN) when length(Workers) >= TargetN ->
    Workers;
replenish(Module, Workers, Sleep, TargetN) ->
    Peer = hd(Workers),
    Id = next_id(seed_id()) + rand:uniform(100000),
    W = worker:start(Id, Module, 200 + Id, Peer, Sleep),
    timer:sleep(250),
    replenish(Module, [W|Workers], Sleep, TargetN).

%% -------- utils --------
pick_unique(List, K) ->
    Shuffled = shuffle(List),
    take(K, Shuffled).

shuffle(L) ->
    Pairs = [{rand:uniform(), X} || X <- L],
    [X || {_K,X} <- lists:sort(Pairs)].

take(K, L) ->
    lists:sublist(L, min(K, length(L))).

stop_all(Workers) ->
    lists:foreach(
      fun(W) ->
          Ref = erlang:monitor(process, W),
          W ! stop,
          receive {'DOWN', Ref, process, W, _} -> ok
          after 1200 ->
              erlang:demonitor(Ref, [flush]),
              exit(W, kill)
          end
      end, Workers),
    timer:sleep(500),
    ok.

%% produce a monotonic-ish id base per node
seed_id() ->
    {_, S2, S3} = now_time(),
    S2 + S3.

now_time() ->
    erlang:monotonic_time(), erlang:system_time(), erlang:unique_integer([monotonic]).

next_id(Base) -> Base.

%%% node4.erl — Section 4: replication to successors
-module(node4).
-export([start/1, start/2, add/3, lookup/2, probe/1]).

-define(Stabilize, 1000).
-define(Replicate, 5000).
-define(Timeout, 2000).

%% ===== API =====
start(Id) -> start(Id, nil).
start(Id, Peer) -> spawn(fun() -> init(Id, Peer) end).
probe(Pid) -> Pid ! probe, ok.

add(Pid, Key, Val) ->
    Qref = make_ref(),
    Pid ! {add, Key, Val, Qref, self()},
    receive {Qref, ok} -> ok after ?Timeout -> timeout end.

lookup(Pid, Key) ->
    Qref = make_ref(),
    Pid ! {lookup, Key, Qref, self()},
    receive {Qref, Result} -> Result after ?Timeout -> timeout end.

%% ===== Internal =====
init(Id, nil) ->
    schedule_stabilize(), schedule_replicate(),
    Store = storage:create(),
    Succ = {Id, undefined, self()},
    node(Id, nil, Succ, nil, nil, Store);
init(Id, Peer) ->
    {ok, SKey} = connect(Id, Peer),
    schedule_stabilize(), schedule_replicate(),
    Store = storage:create(),
    Succ = set_successor(undefined, {SKey, Peer}),
    node(Id, nil, Succ, nil, nil, Store).

%% State: node(Id, Pred, Succ, Next, Succ2, Store)
node(Id, Pred, Succ, Next, Succ2, Store) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Pred, Succ, Next, Succ2, Store);

        {status, PredMsg, NxMsg} ->
            {Succ1b, Next1b, Succ2b} = stabilize(PredMsg, NxMsg, Id, Succ, Next, Succ2),
            node(Id, Pred, Succ1b, Next1b, Succ2b, Store);

        {notify, New} ->
            {Pred2, Store2} = notify(New, Id, Pred, Store),
            node(Id, Pred2, Succ, Next, Succ2, Store2);

        {request, Peer} ->
            request(Peer, Pred, Next),
            node(Id, Pred, Succ, Next, Succ2, Store);

        stabilize ->
            stabilize(Succ),
            node(Id, Pred, Succ, Next, Succ2, Store);

        replicate ->
            replicate_all(Store, Succ, Succ2),
            node(Id, Pred, Succ, Next, Succ2, Store);

        {add, Key, Val, Qref, Client} ->
            Store2 = do_add(Key, Val, Qref, Client, Id, Pred, Succ, Succ2, Store),
            node(Id, Pred, Succ, Next, Succ2, Store2);

        {lookup, Key, Qref, Client} ->
            do_lookup(Key, Qref, Client, Id, Pred, Succ, Store),
            node(Id, Pred, Succ, Next, Succ2, Store);

        {handover, Elements} ->
            Store2 = storage:merge(Elements, Store),
            node(Id, Pred, Succ, Next, Succ2, Store2);

        {replica_put, K, V} ->
            Store2 = storage:add(K, V, Store),
            node(Id, Pred, Succ, Next, Succ2, Store2);

        {replica_bulk, Elements} ->
            Store2 = storage:merge(Elements, Store),
            node(Id, Pred, Succ, Next, Succ2, Store2);

        {print, From} ->
            From ! {state, Id, Pred, Succ, Next, Succ2, Store},
            node(Id, Pred, Succ, Next, Succ2, Store);

        probe ->
            create_probe(Id, Succ),
            node(Id, Pred, Succ, Next, Succ2, Store);

        {probe, I, Nodes, T} when I =:= Id ->
            %% probe returned to origin
            remove_probe(T, Nodes),
            node(Id, Pred, Succ, Next, Succ2, Store);

        {probe, I, Nodes, T} ->
            %% forward around the ring
            forward_probe(I, T, Nodes, Id, Succ),
            node(Id, Pred, Succ, Next, Succ2, Store);

        {'DOWN', Ref, process, _Pid, _Reason} ->
            {Pred2, SuccA, NextA, Succ2A} = down(Ref, Pred, Succ, Next, Succ2, Id),
            self() ! stabilize,
            node(Id, Pred2, SuccA, NextA, Succ2A, Store)
    end.

%% ---- timers ----
schedule_stabilize() -> timer:send_interval(?Stabilize, self(), stabilize).
schedule_replicate() -> timer:send_interval(?Replicate, self(), replicate).

%% ---- messaging ----
stabilize({_, _Ref, SPid}) -> SPid ! {request, self()}.
request(Peer, nil, Nx) -> Peer ! {status, nil, Nx};
request(Peer, {PKey, _R, PPid}, Nx) -> Peer ! {status, {PKey, PPid}, Nx}.

%% ---- stabilize using successor’s predecessor and next ----
stabilize(PredMsg, NxMsg, Id, Succ = {SKey,_SRef,SPid}, Next, Succ2) ->
    %% adopt better successor if needed
    Succ1 =
      case PredMsg of
        nil -> SPid ! {notify, {Id, self()}}, Succ;
        {Id,_} -> Succ;
        {SKey,_} -> SPid ! {notify, {Id, self()}}, Succ;
        {XKey, XPid} ->
            case key:between(XKey, Id, SKey) of
                true  -> XPid ! {request, self()}, set_successor(Succ, {XKey, XPid});
                false -> SPid ! {notify, {Id, self()}}, Succ
            end
      end,
    %% refresh Next from successor’s advertised Next
    Next1 = case NxMsg of
                nil -> Next;
                {NK1, NP1} -> set_next(Next, {NK1, NP1})
            end,
    %% derive Succ2 from Next’s next hop if present, else keep old
    Succ2_1 = case NxMsg of
                  {NK2, NP2} -> set_succ2(Succ2, {NK2, NP2});
                  _ -> Succ2
              end,
    {Succ1, Next1, Succ2_1}.

notify({NKey, NPid}, Id, nil, Store) ->
    {set_predecessor(nil, {NKey, NPid}), handover(Store, NKey, Id, NPid)};
notify({NKey, NPid}, Id, Pred = {PKey, _R, _P}, Store) ->
    case key:between(NKey, PKey, Id) of
        true  -> {set_predecessor(Pred, {NKey, NPid}), handover(Store, NKey, Id, NPid)};
        false -> {Pred, Store}
    end.

handover(Store, NKey, Id, NPid) ->
    {Keep, Rest} = storage:split(NKey, Id, Store),
    NPid ! {handover, Rest},
    Keep.

connect(Id, nil) -> {ok, Id};
connect(_Id, Peer) ->
    Qref = make_ref(), Peer ! {key, Qref, self()},
    receive {Qref, SKey} -> {ok, SKey}
    after ?Timeout -> exit({timeout_connect, Peer}) end.

%% ---- responsibility ----
responsible(_Key, _Id, nil) -> true;
responsible(Key, Id, {PKey,_R,_P}) -> key:between(Key, PKey, Id).

%% ---- add/lookup with replication ----
do_add(Key, Val, Qref, Client, Id, Pred, Succ, Succ2, Store) ->
    case responsible(Key, Id, Pred) of
        true  ->
            Client ! {Qref, ok},
            Store1 = storage:add(Key, Val, Store),
            replicate_put(Key, Val, Succ, Succ2),
            Store1;
        false ->
            case Succ of
                {_,_,Spid} -> Spid ! {add, Key, Val, Qref, Client};
                _ -> Client ! {Qref, timeout}
            end,
            Store
    end.

do_lookup(Key, Qref, Client, Id, Pred, Succ, Store) ->
    case responsible(Key, Id, Pred) of
        true  ->
            Client ! {Qref, storage:lookup(Key, Store)};
        false ->
            case Succ of
                {_,_,Spid} -> Spid ! {lookup, Key, Qref, Client};
                _ -> Client ! {Qref, timeout}
            end
    end.

%% ---- replication helpers ----
replicate_put(_K,_V, nil, _S2) -> ok;
replicate_put(K,V, {_,_,S1}, nil) ->
    S1 ! {replica_put, K, V};
replicate_put(K,V, {_,_,S1}, {_,_,S2}) ->
    S1 ! {replica_put, K, V},
    S2 ! {replica_put, K, V}.

replicate_all(_Store, nil, _S2) -> ok;
replicate_all(Store, {_,_,S1}, nil) ->
    S1 ! {replica_bulk, Store};
replicate_all(Store, {_,_,S1}, {_,_,S2}) ->
    S1 ! {replica_bulk, Store},
    S2 ! {replica_bulk, Store}.

%% ---- probe ----
create_probe(Id, {_,_,Spid}) ->
    Now = erlang:system_time(microsecond),
    Spid ! {probe, Id, [{Id, self()}], Now}.
forward_probe(I, T, Nodes, Id, {_,_,Spid}) ->
    Spid ! {probe, I, [{Id, self()}|Nodes], T}.
remove_probe(T, Nodes) ->
    Now = erlang:system_time(microsecond),
    DtUs = Now - T,
    io:format("Probe complete. Nodes=~p  time=~pus (~p ms)~n",
              [lists:reverse(Nodes), DtUs, DtUs/1000]).

%% ---- monitors ----
monitor_pid(Pid) when Pid =:= self() -> undefined;
monitor_pid(Pid) -> erlang:monitor(process, Pid).
drop(undefined) -> ok; drop(nil) -> ok; drop(Ref) -> erlang:demonitor(Ref,[flush]).

set_successor(undefined, {K,Pid}) -> {K, monitor_pid(Pid), Pid};
set_successor({K,Ref,Pid}, {K,Pid}) -> {K, Ref, Pid};
set_successor({_,RefOld,_}, {K,Pid}) -> drop(RefOld), {K, monitor_pid(Pid), Pid}.

set_predecessor(nil, {K,Pid}) -> {K, monitor_pid(Pid), Pid};
set_predecessor({K,Ref,Pid}, {K,Pid}) -> {K, Ref, Pid};
set_predecessor({_,RefOld,_}, {K,Pid}) -> drop(RefOld), {K, monitor_pid(Pid), Pid}.

set_next(nil, {K,Pid}) -> {K, Pid};
set_next({K,Pid}, {K,Pid}) -> {K,Pid};
set_next(_Old, {K,Pid}) -> {K,Pid}.

set_succ2(nil, {K,Pid}) -> {K, monitor_pid(Pid), Pid};
set_succ2({K,Ref,Pid}, {K,Pid}) -> {K,Ref,Pid};
set_succ2({_,RefOld,_}, {K,Pid}) -> drop(RefOld), {K, monitor_pid(Pid), Pid}.

%% ---- DOWN handling ----
down(Ref, {_, Ref, _}, Succ, Next, Succ2, _Id) ->
    drop(Ref),
    {nil, Succ, Next, Succ2};
down(Ref, Pred, {_, Ref, _}, nil, Succ2, Id) ->
    drop(Ref),
    {Pred, {Id, undefined, self()}, nil, Succ2};
down(Ref, Pred, {_, Ref, _}, {NK, NPid}, Succ2, _Id) ->
    drop(Ref),
    {Pred, set_successor(undefined, {NK, NPid}), nil, Succ2};
down(_Ref, Pred, Succ, Next, Succ2, _Id) ->
    {Pred, Succ, Next, Succ2}.

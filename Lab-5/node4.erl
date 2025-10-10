%% node4.erl — Chord with predecessor replica and 2-deep successors.
%% State: node(Id, Pred, Succ, Next, Store, Replica)
-module(node4).
-export([start/1, start/2, add/3, lookup/2, probe/1, kill/1]).

-define(Stabilize, 1000).
-define(Replicate, 5000).
-define(Timeout, 2000).

%% ===================== API =====================

start(Id)       -> start(Id, nil).
start(Id, Peer) -> spawn(fun() -> init(Id, Peer) end).
kill(Pid)       -> exit(Pid, kill).
probe(Pid)      -> Pid ! probe, ok.

add(Pid, Key, Val) ->
    Qref = make_ref(),
    Pid ! {add, Key, Val, Qref, self()},
    receive {Qref, ok} -> ok after ?Timeout -> timeout end.

lookup(Pid, Key) ->
    Qref = make_ref(),
    Pid ! {lookup, Key, Qref, self()},
    receive {Qref, Result} -> Result after ?Timeout -> timeout end.

%% ===================== Init =====================

init(Id, nil) ->
    schedule_stabilize(), schedule_replicate(),
    Store   = storage:create(),
    Replica = storage:create(),
    Succ    = set_successor(undefined, {Id, self()}),
    node(Id, nil, Succ, nil, Store, Replica);

init(Id, Peer) ->
    schedule_stabilize(), schedule_replicate(),
    Store   = storage:create(),
    Replica = storage:create(),
    {ok, {SKey, Peer}} = connect(Id, Peer),
    Succ    = set_successor(undefined, {SKey, Peer}),
    node(Id, nil, Succ, nil, Store, Replica).

schedule_stabilize() -> timer:send_interval(?Stabilize, stabilize).
schedule_replicate() -> timer:send_interval(?Replicate, replicate).

%% ===================== Message loop =====================

node(Id, Pred, Succ, Next, Store, Replica) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Pred, Succ, Next, Store, Replica);

        stabilize ->
            stabilize(Succ),
            node(Id, Pred, Succ, Next, Store, Replica);

        {status, PredOfSucc, NextOfSucc} ->
            {Succ1, Next1} = stabilize(PredOfSucc, NextOfSucc, Id, Succ, Next),
            node(Id, Pred, Succ1, Next1, Store, Replica);

        {request, Peer} ->
            %% FIX: publish wire-format for both Pred and Next
            Peer ! {status, pub(Pred), pub(Next)},
            node(Id, Pred, Succ, Next, Store, Replica);

        {notify, {NKey, NPid}} ->
            {Pred1, Store1, Replica1} = notify({NKey, NPid}, Id, Pred, Store, Replica),
            request_dump(Pred1),
            node(Id, Pred1, Succ, Next, Store1, Replica1);

        {add, Key, Val, Qref, Client} ->
            Store2 = do_add(Key, Val, Qref, Client, Id, Pred, Succ, Store),
            node(Id, Pred, Succ, Next, Store2, Replica);

        {lookup, Key, Qref, Client} ->
            do_lookup(Key, Qref, Client, Id, Pred, Succ, Store),
            node(Id, Pred, Succ, Next, Store, Replica);

        {replica, Key, Val} ->
            node(Id, Pred, Succ, Next, Store, storage:add(Key, Val, Replica));

        {dump_request, From} ->
            From ! {store_dump, Store},
            node(Id, Pred, Succ, Next, Store, Replica);

        {store_dump, Elements} ->
            Replica2 = storage:merge(Elements, storage:create()),
            node(Id, Pred, Succ, Next, Store, Replica2);

        {handover, Elements} ->
            node(Id, Pred, Succ, Next, storage:merge(Elements, Store), Replica);

        replicate ->
            replicate_all(Store, Succ),
            node(Id, Pred, Succ, Next, Store, Replica);

        {print, From} ->
            %% keep shape compatible with older tooling: slot6 nil for Succ2
            From ! {state, Id, Pred, Succ, Next, nil, Store},
            node(Id, Pred, Succ, Next, Store, Replica);

        probe ->
            create_probe(Id, Succ),
            node(Id, Pred, Succ, Next, Store, Replica);

        {probe, I, Nodes, T} when I =:= Id ->
            remove_probe(T, Nodes),
            node(Id, Pred, Succ, Next, Store, Replica);

        {probe, I, Nodes, T} ->
            forward_probe(I, T, Nodes, Id, Succ),
            node(Id, Pred, Succ, Next, Store, Replica);

        {'DOWN', Ref, process, _Pid, _Reason} ->
            case which_ref(Ref, Pred, Succ, Next) of
                pred ->
                    %% Merge predecessor’s replica into our store, then clear replica.
                    StoreM   = storage:merge(Replica, Store),
                    Replica0 = storage:create(),
                    node(Id, nil, Succ, Next, StoreM, Replica0);
                succ ->
                    {SuccN, NextN} = promote_successor(Id, Succ, Next),
                    node(Id, Pred, SuccN, NextN, Store, Replica);
                next ->
                    node(Id, Pred, Succ, nil, Store, Replica);
                none ->
                    node(Id, Pred, Succ, Next, Store, Replica)
            end
    end.

%% ===================== Joining =====================

connect(Id, nil) -> {ok, {Id, self()}};
connect(_Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive {Qref, SKey} -> {ok, {SKey, Peer}}
    after ?Timeout -> exit({timeout_connect, Peer})
    end.

%% ===================== Stabilization =====================

stabilize({_,_,SPid}) when is_pid(SPid) -> SPid ! {request, self()};
stabilize(_) -> ok.

stabilize(PredMsg, NxMsg, Id, Succ = {SKey,_SR,SPid}, Next) ->
    %% PredMsg and NxMsg are wire-format 2-tuples or nil
    Succ1 =
        case PredMsg of
            nil            -> SPid ! {notify, {Id, self()}}, Succ;
            {Id,_}         -> Succ;
            {SKey,_}       -> SPid ! {notify, {Id, self()}}, Succ;
            {XKey, XPid}   ->
                case key:between(XKey, Id, SKey) of
                    true  -> XPid ! {request, self()}, set_successor(Succ, {XKey, XPid});
                    false -> SPid ! {notify, {Id, self()}}, Succ
                end
        end,
    Next1 =
        case NxMsg of
            nil        -> Next;
            {NK, NPid} -> set_next(Next, {NK, NPid})
        end,
    {Succ1, Next1}.

%% ===================== Notify & handover =====================

notify({NKey, NPid}, Id, nil, Store, _Replica) ->
    Keep  = handover(Store, NKey, Id, NPid),
    Pred1 = set_predecessor(undefined, {NKey, NPid}),
    {Pred1, Keep, storage:create()};
notify({NKey, NPid}, Id, Pred = {PKey,_,_}, Store, Replica) ->
    case key:between(NKey, PKey, Id) of
        true  ->
            Keep  = handover(Store, NKey, Id, NPid),
            Pred1 = set_predecessor(Pred, {NKey, NPid}),
            {Pred1, Keep, storage:create()};
        false ->
            {Pred, Store, Replica}
    end.

handover(Store, NKey, Id, NPid) ->
    {Keep, Rest} = storage:split(NKey, Id, Store),
    NPid ! {handover, Rest},
    Keep.

%% ===================== Store responsibilities =====================

responsible(_Key, _Id, nil) -> true;  %% single node owns all
responsible(Key, Id, {PKey,_,_}) -> key:between(Key, PKey, Id).

do_add(Key, Val, Qref, Client, Id, Pred, {_,_,Spid}=Succ, Store)
  when is_pid(Spid) ->
    case responsible(Key, Id, Pred) of
        true  ->
            Client ! {Qref, ok},
            Store1 = storage:add(Key, Val, Store),
            send_replica(Key, Val, Succ),
            Store1;
        false ->
            Spid ! {add, Key, Val, Qref, Client},
            Store
    end;
do_add(_K,_V,Qref,Client,_Id,_Pred,_Succ,Store) ->
    Client ! {Qref, timeout},
    Store.

do_lookup(Key, Qref, Client, Id, Pred, {_,_,Spid}, Store) when is_pid(Spid) ->
    case responsible(Key, Id, Pred) of
        true  -> Client ! {Qref, storage:lookup(Key, Store)};
        false -> Spid ! {lookup, Key, Qref, Client}
    end;
do_lookup(_K, Qref, Client, _Id, _Pred, _Succ, _Store) ->
    Client ! {Qref, timeout}.

%% ===================== Replication helpers =====================

send_replica(_K,_V, nil) -> ok;
send_replica(Key, Val, {_,_,Pid}) when is_pid(Pid) ->
    Pid ! {replica, Key, Val}, ok;
send_replica(_,_,_) -> ok.

replicate_all(_Store, nil) -> ok;
replicate_all(Store, {_,_,Pid}) when is_pid(Pid) ->
    lists:foreach(fun({K,V}) -> Pid ! {replica, K, V} end, Store), ok;
replicate_all(_,_) -> ok.

request_dump(nil) -> ok;
request_dump({_,_,Pid}) when is_pid(Pid) ->
    Pid ! {dump_request, self()}, ok;
request_dump(_) -> ok.

%% ===================== Tuple constructors and monitors =====================

%% tuples are {Key, Ref, Pid}
set_predecessor(Old, {K,Pid}) ->
    maybe_demonitor(Old),
    R = erlang:monitor(process, Pid),
    {K,R,Pid}.
set_successor(Old, {K,Pid}) ->
    maybe_demonitor(Old),
    R = erlang:monitor(process, Pid),
    {K,R,Pid}.
set_next(Old, {K,Pid}) ->
    maybe_demonitor(Old),
    R = erlang:monitor(process, Pid),
    {K,R,Pid}.

maybe_demonitor({_,Ref,_}) when is_reference(Ref) -> erlang:demonitor(Ref, [flush]);
maybe_demonitor(_) -> ok.

pub(nil) -> nil;
pub({K,_,Pid}) -> {K,Pid}.

which_ref(Ref, {_,PR,_}, {_,SR,_}, _N) when Ref =:= PR -> pred;
which_ref(Ref, _P,       {_,SR,_}, _N) when Ref =:= SR -> succ;
which_ref(Ref, _P,       _S, {_,NR,_}) when Ref =:= NR -> next;
which_ref(_,   _,        _,        _)                  -> none.

promote_successor(Id, _Succ, Next) ->
    case Next of
        {NK,_NR,NPid} when is_pid(NPid) ->
            { set_successor(undefined, {NK, NPid}), nil };
        _ ->
            { set_successor(undefined, {Id, self()}), nil }
    end.

%% ===================== Probe =====================

create_probe(Id, {_,_,Spid}) when is_pid(Spid) ->
    T0 = erlang:system_time(microsecond),
    Spid ! {probe, Id, [{Id, self()}], T0};
create_probe(_, _) -> ok.

forward_probe(I, T, Nodes, Id, {_,_,Spid}) when is_pid(Spid) ->
    Spid ! {probe, I, [{Id, self()}|Nodes], T};
forward_probe(_,_,_,_,_) -> ok.

remove_probe(T, Nodes) ->
    Now = erlang:system_time(microsecond),
    Dt  = Now - T,
    Ring = lists:reverse(Nodes),
    io:format("Probe complete. Nodes=~p  time=~pus (~p ms)~n",[Ring, Dt, Dt/1000]).

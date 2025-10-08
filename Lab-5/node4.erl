%% node4.erl — Chord (minimal) with monitors, 2-deep successors, replication
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
    Store = storage:create(),
    Succ  = set_successor(undefined, {Id, self()}),
    node(Id, nil, Succ, nil, nil, Store);

init(Id, Peer) ->
    schedule_stabilize(), schedule_replicate(),
    Store = storage:create(),
    {ok, {SKey, Peer}} = connect(Id, Peer),
    Succ  = set_successor(undefined, {SKey, Peer}),
    node(Id, nil, Succ, nil, nil, Store).

schedule_stabilize() -> timer:send_interval(?Stabilize, stabilize).
schedule_replicate() -> timer:send_interval(?Replicate, replicate).

%% ===================== Message loop =====================

node(Id, Pred, Succ, Next, Succ2, Store) ->
    receive
        %% --- key discovery for joins ---
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Pred, Succ, Next, Succ2, Store);

        %% --- stabilization tick / request from us to successor ---
        stabilize ->
            stabilize(Succ),
            node(Id, Pred, Succ, Next, Succ2, Store);

        %% --- successor replies with its predecessor and next ---
        {status, PredOfSucc, NextOfSucc} ->
            {Succ1, Next1, Succ2_1} = stabilize(PredOfSucc, NextOfSucc, Id, Succ, Next, Succ2),
            node(Id, Pred, Succ1, Next1, Succ2_1, Store);

        %% --- a peer asks our status (we're their successor) ---
        {request, Peer} ->
            Peer ! {status, Pred, pub(Next)},
            node(Id, Pred, Succ, Next, Succ2, Store);

        %% --- notify: someone claims to be our predecessor ---
        {notify, {NKey, NPid}} ->
            {Pred1, Store1} = notify({NKey, NPid}, Id, Pred, Store),
            node(Id, Pred1, Succ, Next, Succ2, Store1);

        %% --- store ops (client) ---
        {add, Key, Val, Qref, Client} ->
            Store2 = do_add(Key, Val, Qref, Client, Id, Pred, Succ, Next, Succ2, Store),
            node(Id, Pred, Succ, Next, Succ2, Store2);

        {lookup, Key, Qref, Client} ->
            do_lookup(Key, Qref, Client, Id, Pred, Succ, Store),
            node(Id, Pred, Succ, Next, Succ2, Store);

        %% --- replicas from owners ---
        {replica, Key, Val} ->
            node(Id, Pred, Succ, Next, Succ2, storage:add(Key, Val, Store));

        %% --- range handover from successor that accepted us ---
        {handover, Elements} ->
            node(Id, Pred, Succ, Next, Succ2, storage:merge(Elements, Store));

        %% --- periodic replicate of our entire store to S2 (belt-and-suspenders) ---
        replicate ->
            replicate_all(Store, Next, Succ2),
            node(Id, Pred, Succ, Next, Succ2, Store);

        %% --- diagnostics ---
        {print, From} ->
            From ! {state, Id, Pred, Succ, Next, Succ2, Store},
            node(Id, Pred, Succ, Next, Succ2, Store);

        %% --- probe around the ring ---
        probe ->
            create_probe(Id, Succ),
            node(Id, Pred, Succ, Next, Succ2, Store);

        {probe, I, Nodes, T} when I =:= Id ->
            remove_probe(T, Nodes),
            node(Id, Pred, Succ, Next, Succ2, Store);

        {probe, I, Nodes, T} ->
            forward_probe(I, T, Nodes, Id, Succ),
            node(Id, Pred, Succ, Next, Succ2, Store);

        %% --- failure handling via monitors ---
        {'DOWN', Ref, process, _Pid, _Reason} ->
            case which_ref(Ref, Succ, Next, Succ2) of
                succ  ->
                    {SuccN, NextN, Succ2N} = promote_successor(Id, Succ, Next, Succ2),
                    node(Id, Pred, SuccN, NextN, Succ2N, Store);
                next  ->
                    node(Id, Pred, Succ, nil, Succ2, Store);
                succ2 ->
                    node(Id, Pred, Succ, Next, nil, Store);
                none  ->
                    node(Id, Pred, Succ, Next, Succ2, Store)
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

stabilize(PredMsg, NxMsg, Id, Succ = {SKey,_SR,SPid}, Next, Succ2) ->
    %% choose best successor and keep it monitored
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
    %% set Next and Succ2 from NxMsg atomically
    {Next1, Succ2_1} =
        case NxMsg of
            nil          -> {Next, Succ2};
            {NK, NPid}   -> { set_next(Next, {NK, NPid})
                            , set_succ2(Succ2, {NK, NPid}) }
        end,
    {Succ1, Next1, Succ2_1}.

notify({NKey, NPid}, Id, nil, Store) ->
    Keep = handover(Store, NKey, Id, NPid),
    {{NKey, NPid}, Keep};
notify({NKey, NPid}, Id, Pred = {PKey, _}, Store) ->
    case key:between(NKey, PKey, Id) of
        true  ->
            Keep = handover(Store, NKey, Id, NPid),
            {{NKey, NPid}, Keep};
        false ->
            {Pred, Store}
    end.

handover(Store, NKey, Id, NPid) ->
    {Keep, Rest} = storage:split(NKey, Id, Store),
    NPid ! {handover, Rest},
    Keep.

%% ===================== Store responsibilities =====================

responsible(_Key, _Id, nil) -> true;  %% single node owns all
responsible(Key, Id, {PKey,_}) -> key:between(Key, PKey, Id).

do_add(Key, Val, Qref, Client, Id, Pred, {_,_,Spid}=Succ, Next, Succ2, Store)
  when is_pid(Spid) ->
    case responsible(Key, Id, Pred) of
        true  ->
            %% owner: store and replicate to Next and Succ2
            Client ! {Qref, ok},
            Store1 = storage:add(Key, Val, Store),
            send_replica(Key, Val, Next),
            send_replica(Key, Val, Succ2),
            Store1;
        false ->
            Spid ! {add, Key, Val, Qref, Client},
            Store
    end;
do_add(_K,_V,Qref,Client,_Id,_Pred,_Succ,_Next,_Succ2,Store) ->
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

replicate_all(_Store, nil, _S2) -> ok;
replicate_all(Store, {_,_,NPid}, _S2) when is_pid(NPid) ->
    lists:foreach(fun({K,V}) -> NPid ! {replica, K, V} end, Store), ok;
replicate_all(_,_,_) -> ok.

%% ===================== Successor/Next tuples and monitors =====================

%% tuples are {Key, Ref, Pid}
set_successor(Old, {K,Pid}) ->
    maybe_demonitor(Old),
    R = erlang:monitor(process, Pid),
    {K,R,Pid}.
set_next(Old, {K,Pid}) ->
    maybe_demonitor(Old),
    R = erlang:monitor(process, Pid),
    {K,R,Pid}.
set_succ2(Old, {K,Pid}) ->
    maybe_demonitor(Old),
    R = erlang:monitor(process, Pid),
    {K,R,Pid}.

maybe_demonitor({_,Ref,_}) when is_reference(Ref) -> erlang:demonitor(Ref, [flush]);
maybe_demonitor(_) -> ok.

pub(nil) -> nil;              %% strip ref for wire format
pub({K,_,Pid}) -> {K,Pid}.

which_ref(Ref, {_,SR,_}, _Next, _S2) when Ref =:= SR -> succ;
which_ref(Ref, _S, {_,NR,_}, _S2)    when Ref =:= NR -> next;
which_ref(Ref, _S, _N, {_,R,_})      when Ref =:= R  -> succ2;
which_ref(_,    _,      _,     _)                     -> none.

promote_successor(Id, _Succ, Next, Succ2) ->
    case Next of
        {NK,_NR,NPid} when is_pid(NPid) ->
            { set_successor(undefined, {NK, NPid})
            , Succ2
            , nil };
        _ ->
            { set_successor(undefined, {Id, self()}), nil, nil }
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

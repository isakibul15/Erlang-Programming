%%% node3.erl -- Section 3: failures (successor-of-successor, monitors, down-handling)
-module(node3).
-export([start/1, start/2, add/3, lookup/2, probe/1]).

-define(Stabilize, 1000).
-define(Timeout, 2000).

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

init(Id, nil) ->
    schedule_stabilize(),
    Store = storage:create(),
    Succ = {Id, undefined, self()},
    node(Id, nil, Succ, nil, Store);
init(Id, Peer) ->
    {ok, SKey} = connect(Id, Peer),
    schedule_stabilize(),
    Store = storage:create(),
    Succ = set_successor(undefined, {SKey, Peer}),
    node(Id, nil, Succ, nil, Store).

%% Pred :: nil | {Key, Ref, Pid}
%% Succ :: {Key, Ref, Pid}
%% Next :: nil | {Key, Pid}
node(Id, Predecessor, Successor, Next, Store) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor, Next, Store);

        {status, PredMsg, NxMsg} ->
            {Succ2, Next2} = stabilize(PredMsg, NxMsg, Id, Successor),
            node(Id, Predecessor, Succ2, Next2, Store);

        {notify, NewMsg} ->
            {Pred2, Store2} = notify(NewMsg, Id, Predecessor, Store),
            node(Id, Pred2, Successor, Next, Store2);

        {request, Peer} ->
            request(Peer, Predecessor, Next),
            node(Id, Predecessor, Successor, Next, Store);

        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Next, Store);

        %% store ops
        {add, Key, Val, Qref, Client} ->
            Store2 = do_add(Key, Val, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store2);

        {lookup, Key, Qref, Client} ->
            do_lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store);

        {handover, Elements} ->
            Store2 = storage:merge(Elements, Store),
            node(Id, Predecessor, Successor, Next, Store2);

        %% diagnostics
        {print, From} ->
            From ! {state, Id, Predecessor, Successor, Next, Store},
            node(Id, Predecessor, Successor, Next, Store);

        %% probe
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Next, Store);
        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Next, Store);
        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Next, Store);

        %% failure detection
        {'DOWN', Ref, process, _Pid, _Reason} ->
            {Pred2, Succ2, Next2} = down(Ref, Predecessor, Successor, Next, Id),
            self() ! stabilize,
            node(Id, Pred2, Succ2, Next2, Store)
    end.

%% -------- stabilization --------
schedule_stabilize() -> timer:send_interval(?Stabilize, self(), stabilize).
stabilize({_, _Ref, Spid}) -> Spid ! {request, self()}.

request(Peer, nil, Nx) -> Peer ! {status, nil, Nx};
request(Peer, {PKey, _Ref, PPid}, Nx) -> Peer ! {status, {PKey, PPid}, Nx}.

stabilize(PredMsg, NxMsg, Id, Successor = {SKey, _SRef, SPid}) ->
    case PredMsg of
        nil ->
            SPid ! {notify, {Id, self()}},
            {Successor, NxMsg};
        {Id, _} ->
            {Successor, NxMsg};
        {SKey, _} ->
            SPid ! {notify, {Id, self()}},
            {Successor, NxMsg};
        {XKey, XPid} ->
            case key:between(XKey, Id, SKey) of
                true ->
                    Succ2 = set_successor(Successor, {XKey, XPid}),
                    XPid ! {request, self()},
                    {Succ2, NxMsg};
                false ->
                    SPid ! {notify, {Id, self()}},
                    {Successor, NxMsg}
            end
    end.

%% -------- notify + handover --------
notify({NKey, NPid}, Id, nil, Store) ->
    {set_predecessor(nil, {NKey, NPid}), handover(Store, NKey, Id, NPid)};
notify({NKey, NPid}, Id, Pred = {PKey, _Ref, _Pid}, Store) ->
    case key:between(NKey, PKey, Id) of
        true  -> {set_predecessor(Pred, {NKey, NPid}), handover(Store, NKey, Id, NPid)};
        false -> {Pred, Store}
    end.

handover(Store, NKey, Id, NPid) ->
    {Keep, Rest} = storage:split(NKey, Id, Store),
    NPid ! {handover, Rest},
    Keep.

%% -------- joining --------
connect(Id, nil) -> {ok, Id};
connect(_Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive {Qref, SKey} -> {ok, SKey}
    after ?Timeout -> exit({timeout_connect, Peer}) end.

%% -------- store ops --------
responsible(_Key, _Id, nil) -> true;
responsible(Key, Id, {PKey, _Ref, _Pid}) -> key:between(Key, PKey, Id).

do_add(Key, Val, Qref, Client, Id, Pred, {_, _Ref, Spid}, Store) ->
    case responsible(Key, Id, Pred) of
        true  -> Client ! {Qref, ok}, storage:add(Key, Val, Store);
        false -> Spid ! {add, Key, Val, Qref, Client}, Store
    end.

do_lookup(Key, Qref, Client, Id, Pred, {_, _Ref, Spid}, Store) ->
    case responsible(Key, Id, Pred) of
        true  -> Client ! {Qref, storage:lookup(Key, Store)};
        false -> Spid ! {lookup, Key, Qref, Client}
    end.

%% -------- probe --------
create_probe(Id, {_, _Ref, Spid}) ->
    Now = erlang:system_time(microsecond),
    Spid ! {probe, Id, [{Id, self()}], Now}.

forward_probe(I, T, Nodes, Id, {_, _Ref, Spid}) ->
    Spid ! {probe, I, [{Id, self()} | Nodes], T}.

remove_probe(T, Nodes) ->
    Now = erlang:system_time(microsecond),
    DtUs = Now - T,
    Ring = lists:reverse(Nodes),
    io:format("Probe complete. Nodes=~p  time=~pus (~p ms)~n", [Ring, DtUs, DtUs/1000]).

%% -------- monitors --------
monitor_pid(Pid) when Pid =:= self() -> undefined;
monitor_pid(Pid) -> erlang:monitor(process, Pid).

drop(undefined) -> ok;
drop(nil) -> ok;
drop(Ref) -> erlang:demonitor(Ref, [flush]).

set_successor(undefined, {K,Pid}) -> {K, monitor_pid(Pid), Pid};
set_successor({K,Ref,Pid}, {K,Pid}) -> {K, Ref, Pid};
set_successor({_,RefOld,_}, {K,Pid}) -> drop(RefOld), {K, monitor_pid(Pid), Pid}.

set_predecessor(nil, {K,Pid}) -> {K, monitor_pid(Pid), Pid};
set_predecessor({K,Ref,Pid}, {K,Pid}) -> {K, Ref, Pid};
set_predecessor({_,RefOld,_}, {K,Pid}) -> drop(RefOld), {K, monitor_pid(Pid), Pid}.

%% -------- DOWN handling --------
down(Ref, {_, Ref, _}, Successor, Next, _Id) ->
    drop(Ref),
    {nil, Successor, Next};
down(Ref, Predecessor, {_, Ref, _}, nil, Id) ->
    drop(Ref),
    {Predecessor, {Id, undefined, self()}, nil};
down(Ref, Predecessor, {_, Ref, _}, {NKey, NPid}, _Id) ->
    drop(Ref),
    {Predecessor, set_successor(undefined, {NKey, NPid}), nil};
down(_Ref, Predecessor, Successor, Next, _Id) ->
    {Predecessor, Successor, Next}.

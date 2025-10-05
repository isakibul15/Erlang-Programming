-module(node2).
-export([start/1, start/2, add/3, lookup/2, probe/1]).

-define(Stabilize, 1000).
-define(Timeout, 2000).

%% API
start(Id) -> start(Id, nil).
start(Id, Peer) -> spawn(fun() -> init(Id, Peer) end).
probe(Pid) -> Pid ! probe, ok.

%% Client helpers
add(Pid, Key, Val) ->
    Qref = make_ref(),
    Pid ! {add, Key, Val, Qref, self()},
    receive
        {Qref, ok} -> ok
    after ?Timeout -> timeout
    end.

lookup(Pid, Key) ->
    Qref = make_ref(),
    Pid ! {lookup, Key, Qref, self()},
    receive
        {Qref, Result} -> Result
    after ?Timeout -> timeout
    end.

%% Internal
init(Id, nil) ->
    schedule_stabilize(),
    Store = storage:create(),
    node(Id, nil, {Id, self()}, Store);
init(Id, Peer) ->
    {ok, Succ} = connect(Id, Peer),
    schedule_stabilize(),
    Store = storage:create(),
    node(Id, nil, Succ, Store).

%% Message loop with store
node(Id, Predecessor, Successor, Store) ->
    receive
        %% ring maintenance
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor, Store);

        {status, Pred} ->
            Succ2 = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, Succ2, Store);

        {notify, New} ->
            {Pred2, Store2} = notify(New, Id, Predecessor, Store),
            node(Id, Pred2, Successor, Store2);

        {request, Peer} ->
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor, Store);

        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Store);

        %% store operations
        {add, Key, Val, Qref, Client} ->
            Store2 = do_add(Key, Val, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Store2);

        {lookup, Key, Qref, Client} ->
            do_lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Store);

        {handover, Elements} ->
            Store2 = storage:merge(Elements, Store),
            node(Id, Predecessor, Successor, Store2);

        %% diagnostics
        {print, From} ->
            From ! {state, Id, Predecessor, Successor, Store},
            node(Id, Predecessor, Successor, Store);

        %% probe support
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Store);
        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Store);
        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Store)
    end.

%% -------- stabilization --------
schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, Spid}) ->
    Spid ! {request, self()}.
    
request(Peer, nil) -> Peer ! {status, nil};
request(Peer, Pred = {_K,_Pid}) -> Peer ! {status, Pred}.

%% notify with handover
notify(New = {NKey, NPid}, _Id, nil, Store) ->
    Keep = handover(Store, NKey, _Id = 0, NPid), % _Id not used in split calc here
    {{NKey, NPid}, Keep};
notify(New = {NKey, NPid}, Id, Pred = {PKey, _}, Store) ->
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

%% successor selection
stabilize(PredOfSucc, Id, Successor = {SKey, SPid}) ->
    case PredOfSucc of
        nil ->
            SPid ! {notify, {Id, self()}},
            Successor;
        {Id, _} ->
            Successor;
        {SKey, _} ->
            SPid ! {notify, {Id, self()}},
            Successor;
        {XKey, XPid} ->
            case key:between(XKey, Id, SKey) of
                true ->
                    XPid ! {request, self()},
                    {XKey, XPid};
                false ->
                    SPid ! {notify, {Id, self()}},
                    Successor
            end
    end.

%% -------- joining --------
connect(Id, nil) ->
    {ok, {Id, self()}};
connect(_Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive
        {Qref, SKey} -> {ok, {SKey, Peer}}
    after ?Timeout ->
        exit({timeout_connect, Peer})
    end.

%% -------- store ops --------
responsible(_Key, _Id, nil) ->
    true;  %% one node owns all
responsible(Key, Id, {PKey,_}) ->
    key:between(Key, PKey, Id).

do_add(Key, Val, Qref, Client, Id, Pred, {_, Spid}, Store) ->
    case responsible(Key, Id, Pred) of
        true  ->
            Client ! {Qref, ok},
            storage:add(Key, Val, Store);
        false ->
            Spid ! {add, Key, Val, Qref, Client},
            Store
    end.

do_lookup(Key, Qref, Client, Id, Pred, {_, Spid}, Store) ->
    case responsible(Key, Id, Pred) of
        true  ->
            Client ! {Qref, storage:lookup(Key, Store)};
        false ->
            Spid ! {lookup, Key, Qref, Client}
    end.

%% -------- probe helpers --------
create_probe(Id, {_, Spid}) ->
    Ref = make_ref(),
    Now = erlang:system_time(microsecond),
    Spid ! {probe, Ref, [{Id, self()}], Now}.

forward_probe(Ref, T, Nodes, Id, {_, Spid}) ->
    Spid ! {probe, Ref, [{Id, self()} | Nodes], T}.

remove_probe(T, Nodes) ->
    Now = erlang:system_time(microsecond),
    DtUs = Now - T,
    Ring = lists:reverse(Nodes),
    io:format("Probe complete. Nodes=~p  time=~pus (~p ms)~n",
              [Ring, DtUs, DtUs/1000]).

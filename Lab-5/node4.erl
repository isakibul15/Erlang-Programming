-module(node4).
-export([start/1, start/2]).
-export([init/2, node/6]).
-export([stabilize/1, schedule_stabilize/0, stabilize/3, request/2, notify/3, handover/4]).
-export([add/8, lookup/7]).
-export([create_probe/2, remove_probe/2, forward_probe/5]).

-define(Stabilize, 1000).
-define(Timeout, 5000).

start(Id) ->
    start(Id, nil).

start(Id, Peer) ->
    timer:start(),
    spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
    Predecessor = nil,
    {ok, Successor} = connect(Id, Peer),
    schedule_stabilize(),
    Store = storage:create(),
    Replica = storage:create(),
    node(Id, Predecessor, Successor, Store, Replica).

connect(_Id, nil) ->
    {ok, {nil, nil, self()}};  % First node - special case
connect(_Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive
        {Qref, Skey} ->
            {ok, {Skey, monitor(Peer), Peer}}
    after ?Timeout ->
            io:format("Time out: no response~n",[]),
            {error, timeout}
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

node(Id, Predecessor, Successor, Store, Replica) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor, Store, Replica);
            
        {notify, New} ->
            {NewPred, NewStore} = notify(New, Id, Predecessor, Store),
            node(Id, NewPred, Successor, NewStore, Replica);
            
        {request, Peer} ->
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor, Store, Replica);
            
        {status, Pred} ->
            NewSucc = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, NewSucc, Store, Replica);
            
        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Store, Replica);
            
        {add, Key, Value, Qref, Client} ->
            NewStore = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, NewStore, Replica);
            
        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store, Replica),
            node(Id, Predecessor, Successor, Store, Replica);
            
        {handover, Elements} ->
            Merged = storage:merge(Elements, Store),
            node(Id, Predecessor, Successor, Merged, Replica);
            
        {replicate, Key, Value} ->
            % Add to replica store
            NewReplica = storage:add(Key, Value, Replica),
            node(Id, Predecessor, Successor, Store, NewReplica);
            
        {'DOWN', Ref, process, _, _} ->
            {NewPred, NewSucc, NewStore} = down(Ref, Predecessor, Successor, Store, Replica),
            node(Id, NewPred, NewSucc, NewStore, storage:create());
            
        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Store, Replica);
            
        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Store, Replica);
            
        stop ->
            ok;
            
        _ ->
            node(Id, Predecessor, Successor, Store, Replica)
    end.

%% Replication: When adding a key, also replicate to successor
add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store) ->
    case Predecessor of
        nil ->
            % We are the only node, just add locally
            Client ! {Qref, ok},
            NewStore = storage:add(Key, Value, Store),
            % Replicate to our own replica (since we're the only node)
            self() ! {replicate, Key, Value},
            NewStore;
        {Pkey, _Pref, _Ppid} ->
            case key:between(Key, Pkey, Id) of
                true ->
                    % This key belongs to us
                    Client ! {Qref, ok},
                    NewStore = storage:add(Key, Value, Store),
                    % Replicate to successor
                    case Successor of
                        {_Skey, _Sref, Spid} when Spid =/= self() ->
                            Spid ! {replicate, Key, Value};
                        _ ->
                            ok
                    end,
                    NewStore;
                false ->
                    % Forward to successor
                    case Successor of
                        {_Skey, _Sref, Spid} ->
                            Spid ! {add, Key, Value, Qref, Client};
                        _ ->
                            Client ! {Qref, error}
                    end,
                    Store
            end
    end.

lookup(Key, Qref, Client, Id, Predecessor, Successor, Store, Replica) ->
    case Predecessor of
        nil ->
            % We are the only node, check our store and replica
            case storage:lookup(Key, Store) of
                false ->
                    % Check replica if not found in main store
                    case storage:lookup(Key, Replica) of
                        false -> Client ! {Qref, false};
                        Result -> Client ! {Qref, Result}
                    end;
                Result ->
                    Client ! {Qref, Result}
            end;
        {Pkey, _, _} ->
            case key:between(Key, Pkey, Id) of
                true ->
                    % Check both store and replica
                    case storage:lookup(Key, Store) of
                        false ->
                            % Check replica
                            case storage:lookup(Key, Replica) of
                                false -> Client ! {Qref, false};
                                Result -> Client ! {Qref, Result}
                            end;
                        Result ->
                            Client ! {Qref, Result}
                    end;
                false ->
                    % Forward to successor
                    case Successor of
                        {_Skey, _Sref, Spid} ->
                            Spid ! {lookup, Key, Qref, Client};
                        _ ->
                            Client ! {Qref, false}
                    end
            end
    end.

stabilize({_, _, Spid}) ->
    Spid ! {request, self()}.

stabilize(Pred, Id, Successor) ->
    case Pred of
        nil ->
            Successor;
        {Id, _} ->
            Successor;
        {Skey, _} ->
            Successor;
        {Xkey, _Xref, Xpid} ->
            {Skey, _Sref, _Spid} = Successor,
            case key:between(Xkey, Id, Skey) of
                true ->
                    % Adopt this node as our successor
                    {Xkey, monitor(Xpid), Xpid};
                false ->
                    Successor
            end
    end.

request(Peer, Predecessor) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil};
        {Pkey, Pref, Ppid} ->
            Peer ! {status, {Pkey, Pref, Ppid}}
    end.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
    case Predecessor of
        nil ->
            % Accept the new node as our predecessor
            NewRef = monitor(Npid),
            Keep = handover(Id, Store, Nkey, Npid),
            {{Nkey, NewRef, Npid}, Keep};
        {Pkey, Pref, _Ppid} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    % New node should be our predecessor
                    drop(Pref),
                    NewRef = monitor(Npid),
                    Keep = handover(Id, Store, Nkey, Npid),
                    {{Nkey, NewRef, Npid}, Keep};
                false ->
                    {Predecessor, Store}
            end
    end.

handover(Id, Store, Nkey, Npid) ->
    {Keep, Rest} = storage:split(Id, Nkey, Store),
    Npid ! {handover, Rest},
    Keep.

down(Ref, Predecessor, Successor, Store, Replica) ->
    case Predecessor of
        {_, Ref, _} ->
            % Predecessor died - merge replica into store
            NewStore = storage:merge(Replica, Store),
            {nil, Successor, NewStore};
        {_Skey, Sref, _Spid} when Sref == Ref ->
            % Successor died - we need to find a new one
            % For simplicity, we'll set successor to ourselves and let stabilization fix it
            {Predecessor, {nil, nil, self()}, Store};
        _ ->
            {Predecessor, Successor, Store}
    end.

monitor(Pid) when is_pid(Pid) ->
    erlang:monitor(process, Pid).

drop(nil) ->
    ok;
drop(Ref) when is_reference(Ref) ->
    erlang:demonitor(Ref, [flush]);
drop(_) ->
    ok.

%% Probe functions
create_probe(Id, Successor) ->
    {_Skey, _Sref, Spid} = Successor,
    Spid ! {probe, Id, [Id], erlang:system_time(micro_seconds)}.

remove_probe(T, Nodes) ->
    Duration = erlang:system_time(micro_seconds) - T,
    io:format("Probe completed: ~w nodes, time ~w microseconds~n", [length(Nodes), Duration]).

forward_probe(Ref, T, Nodes, Id, Successor) ->
    {_Skey, _Sref, Spid} = Successor,
    Spid ! {probe, Ref, [Id | Nodes], T}.
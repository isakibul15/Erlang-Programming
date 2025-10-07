-module(node4).
-export([start/1, start/2]).
-export([init/2, node/6]).
-export([stabilize/1, schedule_stabilize/0, stabilize/4, request/2, notify/4, handover/4]).
-export([add/8, lookup/7, replicate/3]).
-export([create_probe/2, remove_probe/2, forward_probe/5]).

-define(Stabilize, 1000).
-define(Timeout, 5000).

start(Id) ->
    start(Id, nil).

start(Id, Peer) ->
    register(node, self()),
    timer:start(),
    spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
    Predecessor = nil,
    {ok, Successor} = connect(Id, Peer),
    schedule_stabilize(),
    Store = storage:create(),
    Replica = storage:create(),
    node(Id, Predecessor, Successor, Successor, Store, Replica).

connect(Id, nil) ->
    {ok, {Id, self(), self()}};
connect(Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive
        {Qref, Skey} ->
            {ok, {Skey, monitor(Peer), Peer}}
    after ?Timeout ->
            io:format("Time out: no response~n",[])
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

node(Id, Predecessor, Successor, Next, Store, Replica) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor, Next, Store, Replica);
            
        {notify, New, NewStore} ->
            {NewPred, NewStore1} = notify(New, NewStore, Id, Predecessor, Store),
            node(Id, NewPred, Successor, Next, NewStore1, Replica);
            
        {request, Peer} ->
            request(Peer, Predecessor, Store),
            node(Id, Predecessor, Successor, Next, Store, Replica);
            
        {status, Pred, Nx, PredStore} ->
            {Succ, Nxt} = stabilize(Pred, Nx, Id, Successor),
            % Update replica with predecessor's store
            NewReplica = storage:merge(PredStore, Replica),
            node(Id, Predecessor, Succ, Nxt, Store, NewReplica);
            
        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);
            
        {add, Key, Value, Qref, Client} ->
            Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Added, Replica);
            
        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store, Replica);
            
        {handover, Elements} ->
            Merged = storage:merge(Elements, Store),
            node(Id, Predecessor, Successor, Next, Merged, Replica);
            
        {replicate, Key, Value} ->
            % Add to replica store
            NewReplica = storage:add(Key, Value, Replica),
            node(Id, Predecessor, Successor, Next, Store, NewReplica);
            
        {'DOWN', Ref, process, _, _} ->
            {Pred, Succ, Nxt, NewStore} = down(Ref, Predecessor, Successor, Next, Store, Replica),
            node(Id, Pred, Succ, Nxt, NewStore, storage:create());
            
        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Next, Store, Replica);
            
        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);
            
        stop ->
            ok;
            
        _ ->
            node(Id, Predecessor, Successor, Next, Store, Replica)
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
        {Pkey, Pref, Ppid} ->
            case key:between(Key, Pkey, Id) of
                true ->
                    % This key belongs to us
                    Client ! {Qref, ok},
                    NewStore = storage:add(Key, Value, Store),
                    % Replicate to successor
                    case Successor of
                        {Skey, Sref, Spid} when Spid =/= self() ->
                            Spid ! {replicate, Key, Value};
                        _ ->
                            ok
                    end,
                    NewStore;
                false ->
                    % Forward to successor
                    case Successor of
                        {Skey, Sref, Spid} ->
                            Spid ! {add, Key, Value, Qref, Client};
                        _ ->
                            Client ! {Qref, error}
                    end,
                    Store
            end
    end.

lookup(Key, Qref, Client, Id, Predecessor, Successor, Store) ->
    case Predecessor of
        nil ->
            % We are the only node, check our store
            Result = storage:lookup(Key, Store),
            Client ! {Qref, Result};
        {Pkey, _, _} ->
            case key:between(Key, Pkey, Id) of
                true ->
                    % Check both store and replica
                    case storage:lookup(Key, Store) of
                        false ->
                            Client ! {Qref, false};
                        Result ->
                            Client ! {Qref, Result}
                    end;
                false ->
                    % Forward to successor
                    case Successor of
                        {Skey, Sref, Spid} ->
                            Spid ! {lookup, Key, Qref, Client};
                        _ ->
                            Client ! {Qref, false}
                    end
            end
    end.

%% Replicate key-value pair (for replication mechanism)
replicate(Key, Value, Store) ->
    storage:add(Key, Value, Store).

stabilize({_, _, Spid}) ->
    Spid ! {request, self()}.

stabilize(Pred, Next, Id, Successor) ->
    {Skey, Sref, Spid} = Successor,
    case Pred of
        nil ->
            Spid ! {notify, {Id, self()}, storage:create()},
            {Successor, Next};
        {Id, _} ->
            {Successor, Next};
        {Skey, _} ->
            Spid ! {notify, {Id, self()}, storage:create()},
            {Successor, Next};
        {Xkey, Xref, Xpid} ->
            case key:between(Xkey, Id, Skey) of
                true ->
                    drop(Sref),
                    NewRef = monitor(Xpid),
                    {Xkey, NewRef, Xpid} = Pred,
                    stabilize(Pred, Next, Id, Pred);
                false ->
                    Spid ! {notify, {Id, self()}, storage:create()},
                    {Successor, Next}
            end
    end.

request(Peer, Predecessor, Store) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil, nil, storage:create()};
        {Pkey, Pref, Ppid} ->
            Peer ! {status, Predecessor, {Pkey, Pref, Ppid}, Store}
    end.

notify({Nkey, Npid}, NewStore, Id, Predecessor, Store) ->
    case Predecessor of
        nil ->
            NewRef = monitor(Npid),
            Keep = handover(Id, Store, Nkey, Npid),
            {{Nkey, NewRef, Npid}, Keep};
        {Pkey, Pref, Ppid} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
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

down(Ref, Predecessor, Successor, Next, Store, Replica) ->
    case Predecessor of
        {_, Ref, _} ->
            % Predecessor died - merge replica into store
            NewStore = storage:merge(Store, Replica),
            {nil, Successor, Next, NewStore};
        {Skey, Sref, Spid} when Sref == Ref ->
            % Successor died - use next as new successor
            case Next of
                {Nkey, Nref, Npid} ->
                    NewRef = monitor(Npid),
                    {{Nkey, NewRef, Npid}, Next, Next, Store};
                _ ->
                    {Predecessor, Successor, Next, Store}
            end;
        _ ->
            {Predecessor, Successor, Next, Store}
    end.

monitor(Pid) when is_pid(Pid) ->
    erlang:monitor(process, Pid).

drop(nil) ->
    ok;
drop(Ref) ->
    erlang:demonitor(Ref, [flush]).

%% Probe functions
create_probe(Id, Successor) ->
    {Skey, Sref, Spid} = Successor,
    Spid ! {probe, Id, [Id], erlang:system_time(micro_seconds)}.

remove_probe(T, Nodes) ->
    Duration = erlang:system_time(micro_seconds) - T,
    io:format("Probe completed: ~w nodes, time ~w microseconds~n", [length(Nodes), Duration]).

forward_probe(Ref, T, Nodes, Id, Successor) ->
    {Skey, Sref, Spid} = Successor,
    Spid ! {probe, Ref, [Id | Nodes], T}.
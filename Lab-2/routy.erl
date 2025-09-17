-module(routy).
-export([
    start/2, stop/1,
    add/3, remove/2, status/1,
    update/1, broadcast/1, send/3
]).

%% ───────── Public API (convenience wrappers) ─────────

start(Reg, Name) ->
    register(Reg, spawn(fun() -> init(Name) end)).

stop(Reg) ->
    Reg ! stop,
    unregister(Reg),
    ok.

add(Reg, Node, PidOrRemoteName) ->
    Reg ! {add, Node, PidOrRemoteName},
    ok.

remove(Reg, Node) ->
    Reg ! {remove, Node},
    ok.

status(Reg) ->
    Reg ! {status, self()},
    receive
        {status, S} -> S
    after 1000 ->
        timeout
    end.

update(Reg) ->
    Reg ! update,
    ok.

broadcast(Reg) ->
    Reg ! broadcast,
    ok.

send(Reg, To, Message) ->
    Reg ! {send, To, Message},
    ok.

%% ───────── Internals ─────────

init(Name) ->
    Intf  = interfaces:new(),
    Map   = map:new(),
    %% Gateways empty initially; first useful table will be after broadcast+update
    Table = dijkstra:table([], Map),
    Hist  = hist:new(Name),
    router(Name, 0, Hist, Intf, Table, Map).

router(Name, N, Hist, Intf, Table, Map) ->
    receive

        %% Add a neighbor: PidOrRemoteName is either <0.X.Y> or {Reg, 'node@host'}
        {add, Node, PidOrRemoteName} ->
            Ref   = erlang:monitor(process, PidOrRemoteName),
            Intf1 = interfaces:add(Node, Ref, PidOrRemoteName, Intf),
            router(Name, N, Hist, Intf1, Table, Map);

        %% Remove a neighbor explicitly
        {remove, Node} ->
            case interfaces:ref(Node, Intf) of
                {ok, Ref} -> erlang:demonitor(Ref);
                notfound  -> ok
            end,
            Intf1 = interfaces:remove(Node, Intf),
            router(Name, N, Hist, Intf1, Table, Map);

        %% Monitor fired: the neighbor died or never existed on this node
        {'DOWN', Ref, process, _Pid, _Reason} ->
            case interfaces:name(Ref, Intf) of
                {ok, Down} ->
                    io:format("~p: exit received from ~p~n", [Name, Down]),
                    Intf1 = interfaces:remove(Down, Intf),
                    router(Name, N, Hist, Intf1, Table, Map);
                notfound ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;

        %% Inspect full state (for debugging)
        {status, From} ->
            From ! {status, {Name, N, Hist, Intf, Table, Map}},
            router(Name, N, Hist, Intf, Table, Map);

        %% Link-state message received: dedup via history, flood, and update map
        {links, Node, R, Links} ->
            case hist:update(Node, R, Hist) of
                {new, Hist1} ->
                    interfaces:broadcast({links, Node, R, Links}, Intf),
                    Map1 = map:update(Node, Links, Map),
                    %% Keep manual update; recompute only when user sends 'update'
                    router(Name, N, Hist1, Intf, Table, Map1);
                old ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;

        %% Manually recompute routing table (Dijkstra)
        update ->
            Gateways = interfaces:list(Intf),
            Table1   = dijkstra:table(Gateways, Map),
            router(Name, N, Hist, Intf, Table1, Map);

        %% Manually broadcast our current links (and bump our sequence counter)
        broadcast ->
            Links   = interfaces:list(Intf),
            Message = {links, Name, N, Links},
            interfaces:broadcast(Message, Intf),
            router(Name, N+1, Hist, Intf, Table, Map);

        %% Destination case FIRST (message arrived)
        {route, To, From, Message} when To =:= Name ->
            io:format("~p: received message ~s from ~p~n", [Name, Message, From]),
            router(Name, N, Hist, Intf, Table, Map);

        %% Forwarding case
        {route, To, From, Message} ->
            io:format("~p: routing message ~s to ~p~n", [Name, Message, To]),
            case dijkstra:route(To, Table) of
                {ok, Gw} ->
                    case interfaces:lookup(Gw, Intf) of
                        {ok, PidOrRemoteName} ->
                            PidOrRemoteName ! {route, To, From, Message};
                        notfound ->
                            ok
                    end;
                notfound ->
                    ok
            end,
            router(Name, N, Hist, Intf, Table, Map);

        %% Local injection helper
        {send, To, Message} ->
            self() ! {route, To, Name, Message},
            router(Name, N, Hist, Intf, Table, Map);

        stop ->
            ok
    end.

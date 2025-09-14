-module(routy).
-export([start/2, stop/1, add/3, remove/2, status/1, update/1, broadcast/1, send/3]).

start(Reg, Name) ->
    register(Reg, spawn(fun() -> init(Name) end)).

stop(Node) ->
    Node ! stop,
    unregister(Node).

init(Name) ->
    Intf = interfaces:new(),
    Map = map:new(),
    Table = dijkstra:table([], Map), % Empty gateways initially
    Hist = hist:new(Name),
    router(Name, 0, Hist, Intf, Table, Map).

router(Name, N, Hist, Intf, Table, Map) ->
    receive
        {add, Node, Pid} ->
            Ref = erlang:monitor(process, Pid),
            Intf1 = interfaces:add(Node, Ref, Pid, Intf),
            router(Name, N, Hist, Intf1, Table, Map);
        
        {remove, Node} ->
            case interfaces:ref(Node, Intf) of
                {ok, Ref} ->
                    erlang:demonitor(Ref),
                    Intf1 = interfaces:remove(Node, Intf),
                    router(Name, N, Hist, Intf1, Table, Map);
                notfound ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;
        
        {'DOWN', Ref, process, _Pid, _Reason} ->
            case interfaces:name(Ref, Intf) of
                {ok, Down} ->
                    io:format("~s: exit received from ~s~n", [Name, Down]),
                    Intf1 = interfaces:remove(Down, Intf),
                    router(Name, N, Hist, Intf1, Table, Map);
                notfound ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;
        
        {status, From} ->
            From ! {status, {Name, N, Hist, Intf, Table, Map}},
            router(Name, N, Hist, Intf, Table, Map);
        
        {links, Node, R, Links} ->
            case hist:update(Node, R, Hist) of
                {new, Hist1} ->
                    interfaces:broadcast({links, Node, R, Links}, Intf),
                    Map1 = map:update(Node, Links, Map),
                    router(Name, N, Hist1, Intf, Table, Map1);
                old ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;
        
        update ->
            Gateways = interfaces:list(Intf),
            Table1 = dijkstra:table(Gateways, Map),
            router(Name, N, Hist, Intf, Table1, Map);
        
        broadcast ->
            Links = interfaces:list(Intf),
            Message = {links, Name, N, Links},
            interfaces:broadcast(Message, Intf),
            router(Name, N+1, Hist, Intf, Table, Map);
        
        {route, To, From, Message} when To =:= Name ->
            io:format("~s: received message ~w from ~s~n", [Name, Message, From]),
            router(Name, N, Hist, Intf, Table, Map);
        
        {route, To, From, Message} ->
            io:format("~s: routing message ~w to ~s~n", [Name, Message, To]),
            case dijkstra:route(To, Table) of
                {ok, Gw} ->
                    case interfaces:lookup(Gw, Intf) of
                        {ok, Pid} ->
                            Pid ! {route, To, From, Message};
                        notfound ->
                            ok
                    end;
                notfound ->
                    ok
            end,
            router(Name, N, Hist, Intf, Table, Map);
        
        {send, To, Message} ->
            self() ! {route, To, Name, Message},
            router(Name, N, Hist, Intf, Table, Map);
        
        stop ->
            ok
    end.

% Helper functions for external API
add(Reg, Node, Pid) ->
    Reg ! {add, Node, Pid}.

remove(Reg, Node) ->
    Reg ! {remove, Node}.

status(Reg) ->
    Reg ! {status, self()},
    receive
        {status, Status} -> Status
    end.

update(Reg) ->
    Reg ! update.

broadcast(Reg) ->
    Reg ! broadcast.

send(Reg, To, Message) ->
    Reg ! {send, To, Message}.
%% =========================================================
%% routy.erl  —  A tiny link-state router (assignment-ready)
%% =========================================================
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

add(Reg, Node, PidOrRemote) ->
    Reg ! {add, Node, PidOrRemote},
    ok.

remove(Reg, Node) ->
    Reg ! {remove, Node},
    ok.

status(Reg) ->
    Reg ! {status, self()},
    receive
        {status, S} -> S
    after 3000 ->              %% a little more patient across nodes
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
    ok = io:setopts([{encoding, utf8}]),
    Intf  = interfaces:new(),      %% neighbor address book
    Map   = map:new(),             %% network map
    Table = dijkstra:table([], Map),
    Hist  = hist:new(Name),        %% duplicate filter (own name set old)
    router(Name, 0, Hist, Intf, Table, Map).

router(Name, N, Hist, Intf, Table, Map) ->
    receive
        %% Add a neighbor (Pid or {Reg,'node@host'})
        {add, Node, PidOrRemote} ->
            case valid_pidref(PidOrRemote) of
                true ->
                    Ref   = erlang:monitor(process, PidOrRemote),
                    Intf1 = interfaces:add(Node, Ref, PidOrRemote, Intf),
                    router(Name, N, Hist, Intf1, Table, Map);
                false ->
                    %% ignore bad arg silently (assignment keeps smiling)
                    router(Name, N, Hist, Intf, Table, Map)
            end;

        %% Explicit remove
        {remove, Node} ->
            case interfaces:ref(Node, Intf) of
                {ok, Ref} -> erlang:demonitor(Ref);
                notfound  -> ok
            end,
            Intf1 = interfaces:remove(Node, Intf),
            %% recompute locally so table reflects changed gateways
            Table1 = dijkstra:table(interfaces:list(Intf1), Map),
            router(Name, N, Hist, Intf1, Table1, Map);

        %% A monitored neighbor died (or never existed)
        {'DOWN', Ref, process, _Pid, _Reason} ->
            case interfaces:name(Ref, Intf) of
                {ok, Down} ->
                    io:format("~p: exit received from ~p~n", [Name, Down]),
                    Intf1  = interfaces:remove(Down, Intf),
                    Table1 = dijkstra:table(interfaces:list(Intf1), Map),
                    router(Name, N, Hist, Intf1, Table1, Map);
                notfound ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;

        %% Peek at full state
        {status, From} ->
            From ! {status, {Name, N, Hist, Intf, Table, Map}},
            router(Name, N, Hist, Intf, Table, Map);

        %% Link-state flooding (receive)
        {links, Node, R, Links} ->
            case hist:update(Node, R, Hist) of
                {new, Hist1} ->
                    %% forward flood
                    interfaces:broadcast({links, Node, R, Links}, Intf),
                    %% update our map
                    Map1   = map:update(Node, Links, Map),
                    %% fast local recompute (no broadcast here!)
                    Table1 = dijkstra:table(interfaces:list(Intf), Map1),
                    router(Name, N, Hist1, Intf, Table1, Map1);
                old ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;

        %% Manual recompute (Dijkstra)
        update ->
            Table1 = dijkstra:table(interfaces:list(Intf), Map),
            router(Name, N, Hist, Intf, Table1, Map);

        %% Manual broadcast of our current links (and bump seq)
        broadcast ->
            Links   = interfaces:list(Intf),
            Message = {links, Name, N, Links},
            interfaces:broadcast(Message, Intf),
            router(Name, N+1, Hist, Intf, Table, Map);

        %% Deliver at destination
        {route, To, From, Message} when To =:= Name ->
            io:format("~p: received message ~ts from ~p~n",
                      [Name, Message, From]),
            router(Name, N, Hist, Intf, Table, Map);

        %% Forward en route
        {route, To, From, Message} ->
            io:format("~p: routing message ~ts to ~p~n",
                      [Name, Message, To]),
            case dijkstra:route(To, Table) of
                {ok, Gw} ->
                    case interfaces:lookup(Gw, Intf) of
                        {ok, PidOrRemote} ->
                            PidOrRemote ! {route, To, From, Message};
                        notfound ->
                            ok
                    end;
                notfound ->
                    ok
            end,
            router(Name, N, Hist, Intf, Table, Map);

        %% Local injection
        {send, To, Message} ->
            self() ! {route, To, Name, Message},
            router(Name, N, Hist, Intf, Table, Map);

        stop ->
            ok
    end.

%% ───────── Helpers ─────────

%% Accept either a pid() or a remote {Reg,'node@host'} tuple.
valid_pidref(P) when is_pid(P) -> true;
valid_pidref({Reg, Node}) when is_atom(Reg), is_atom(Node) -> true;
valid_pidref(_) -> false.

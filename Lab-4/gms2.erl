-module(gms2).
-export([start/1, start/2]).

%% Start the first node in the group (becomes leader)
start(Id) ->
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Self) end)}.

init(Id, Master) ->
    leader(Id, Master, [], [Master]).

%% Start a node that joins an existing group
start(Id, Grp) ->
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Grp, Self) end)}.

init(Id, Grp, Master) ->
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader, Slaves, Group)
    after 5000 ->  % Timeout after 5 seconds
        Master ! {error, "no reply from leader"}
    end.

%% Leader process implementation
leader(Id, Master, Slaves, Group) ->
    receive
        %% Multicast message from application or peer
        {mcast, Msg} ->
            bcast({msg, Msg}, Slaves),
            Master ! Msg,
            leader(Id, Master, Slaves, Group);
        
        %% Join request from new node
        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            bcast({view, [self()|Slaves2], Group2}, Slaves2),
            Master ! {view, Group2},
            leader(Id, Master, Slaves2, Group2);
        
        stop ->
            ok;
        
        Unexpected ->
            io:format("leader ~w: unexpected message ~w~n", [Id, Unexpected]),
            leader(Id, Master, Slaves, Group)
    end.

%% Slave process implementation
slave(Id, Master, Leader, Slaves, Group) ->
    receive
        %% Multicast message from application - forward to leader
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, Slaves, Group);
        
        %% Join request from application - forward to leader
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, Slaves, Group);
        
        %% Message from leader - deliver to application
        {msg, Msg} ->
            Master ! Msg,
            slave(Id, Master, Leader, Slaves, Group);
        
        %% New view from leader - update and deliver to application
        {view, [Leader|Slaves2], Group2} ->
            Master ! {view, Group2},
            slave(Id, Master, Leader, Slaves2, Group2);
        
        %% Leader down - start election
        {'DOWN', _Ref, process, Leader, _Reason} ->
            io:format("Node ~w: Leader ~w is down, starting election~n", [Id, Leader]),
            election(Id, Master, Slaves, Group);
        
        stop ->
            ok;
        
        Unexpected ->
            io:format("slave ~w: unexpected message ~w~n", [Id, Unexpected]),
            slave(Id, Master, Leader, Slaves, Group)
    end.

%% Election process
election(Id, Master, Slaves, Group) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->  % I am the new leader
            io:format("Node ~w: I am the new leader~n", [Id]),
            bcast({view, [Self|Rest], Group}, Rest),
            Master ! {view, Group},
            leader(Id, Master, Rest, Group);
        [Leader|Rest] ->  % Elect the first slave as new leader
            io:format("Node ~w: Electing ~w as new leader~n", [Id, Leader]),
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader, Rest, Group)
    end.

%% Helper function to broadcast message to all nodes
bcast(Msg, Nodes) ->
    lists:foreach(fun(Node) -> Node ! Msg end, Nodes).
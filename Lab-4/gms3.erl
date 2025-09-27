-module(gms3).
-export([start/1, start/2]).

%% Start the first node in the group (becomes leader)
start(Id) ->
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Self) end)}.

init(Id, Master) ->
    leader(Id, Master, 1, [], [Master]).

%% Start a node that joins an existing group
start(Id, Grp) ->
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Grp, Self) end)}.

init(Id, Grp, Master) ->
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view, N, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader, N+1, {view, N, [Leader|Slaves], Group}, Slaves, Group)
    after 5000 ->
        Master ! {error, "no reply from leader"}
    end.

%% Leader process implementation with sequence numbers
leader(Id, Master, W, Slaves, Group) ->
    receive
        %% Multicast message from application or peer
        {mcast, Msg} ->
            bcast({msg, W, Msg}, Slaves),
            Master ! Msg,
            leader(Id, Master, W+1, Slaves, Group);
        
        %% Join request from new node
        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            bcast({view, W, [self()|Slaves2], Group2}, Slaves2),
            Master ! {view, Group2},
            leader(Id, Master, W+1, Slaves2, Group2);
        
        stop ->
            ok;
        
        Unexpected ->
            io:format("leader ~w: unexpected message ~w~n", [Id, Unexpected]),
            leader(Id, Master, W, Slaves, Group)
    end.

%% Slave process implementation with sequence numbers
slave(Id, Master, Leader, W, Last, Slaves, Group) ->
    receive
        %% Multicast message from application - forward to leader
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, W, Last, Slaves, Group);
        
        %% Join request from application - forward to leader
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, W, Last, Slaves, Group);
        
        %% Message from leader with sequence number
        {msg, I, _} when I < W ->  % Duplicate message - ignore
            slave(Id, Master, Leader, W, Last, Slaves, Group);
        
        {msg, I, Msg} when I == W ->  % Expected message
            Master ! Msg,
            slave(Id, Master, Leader, W+1, {msg, I, Msg}, Slaves, Group);
        
        %% View message from leader with sequence number
        {view, I, [NewLeader|Slaves2], Group2} when I < W ->  % Duplicate view - ignore
            slave(Id, Master, Leader, W, Last, Slaves, Group);
        
        {view, I, [NewLeader|Slaves2], Group2} when I == W ->  % Expected view
            Master ! {view, Group2},
            %% Update monitor to new leader if it changed
            erlang:demonitor(process, Leader),
            erlang:monitor(process, NewLeader),
            slave(Id, Master, NewLeader, W+1, {view, I, [NewLeader|Slaves2], Group2}, Slaves2, Group2);
        
        %% Leader down - start election
        {'DOWN', _Ref, process, Leader, _Reason} ->
            io:format("Node ~w: Leader ~w is down, starting election~n", [Id, Leader]),
            election(Id, Master, W, Last, Slaves, Group);
        
        stop ->
            ok;
        
        Unexpected ->
            io:format("slave ~w: unexpected message ~w~n", [Id, Unexpected]),
            slave(Id, Master, Leader, W, Last, Slaves, Group)
    end.

%% Election process with sequence numbers
election(Id, Master, W, Last, Slaves, Group) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->  % I am the new leader
            io:format("Node ~w: I am the new leader, resending last message~n", [Id]),
            %% Resend the last message to ensure reliability
            bcast(Last, Rest),
            bcast({view, W, [Self|Rest], Group}, Rest),
            Master ! {view, Group},
            leader(Id, Master, W+1, Rest, Group);
        [Leader|Rest] ->  % Elect the first slave as new leader
            io:format("Node ~w: Electing ~w as new leader~n", [Id, Leader]),
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader, W, Last, Rest, Group)
    end.

%% Helper function to broadcast message to all nodes
bcast(Msg, Nodes) ->
    lists:foreach(fun(Node) -> Node ! Msg end, Nodes).
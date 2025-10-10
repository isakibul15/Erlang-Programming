-module(gms4).
-export([start/1, start/2, stop/1]).

%% Start the first node in the group (becomes leader)
start(Id) ->
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Self) end)}.

init(Id, Master) ->
    process_flag(trap_exit, true),
    io:format(">>> NODE ~w: Starting as INITIAL LEADER~n", [Id]),
    self() ! heartbeat,
    leader(Id, Master, 1, [], [Master], #{}).

%% Start a node that joins an existing group  
start(Id, Grp) ->
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Grp, Self) end)}.

init(Id, Grp, Master) ->
    process_flag(trap_exit, true),
    Self = self(),
    io:format(">>> NODE ~w: Joining group via ~w~n", [Id, Grp]),
    reliable_send(Grp, {join, Master, Self}),
    receive
        {view, N, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            MonitorRef = erlang:monitor(process, Leader),
            io:format(">>> NODE ~w: Joined group. Leader: ~w, Slaves: ~w~n", [Id, Leader, Slaves]),
            slave(Id, Master, Leader, MonitorRef, N+1, 
                  {view, N, [Leader|Slaves], Group}, Slaves, Group, #{})
    after 3000 ->
        Master ! {error, "no reply from leader"}
    end.

%% Leader process with strategic logging
leader(Id, Master, W, Slaves, Group, PendingAcks) ->
    receive
        {mcast, Msg} ->
            io:format(">>> LEADER ~w: Multicasting message (seq ~w) to ~w slaves~n", 
                     [Id, W, length(Slaves)]),
            Message = {msg, W, Msg},
            bcast_reliable(Message, Slaves, PendingAcks, W),
            Master ! Msg,
            leader(Id, Master, W+1, Slaves, Group, 
                   PendingAcks#{W => {Message, Slaves, 0}});
        
        {ack, From, Seq} ->
            case PendingAcks of
                #{Seq := {Message, RemainingSlaves, RetryCount}} ->
                    NewRemaining = lists:delete(From, RemainingSlaves),
                    case NewRemaining of
                        [] -> 
                            io:format(">>> LEADER ~w: All ACKs received for seq ~w~n", [Id, Seq]),
                            leader(Id, Master, W, Slaves, Group, maps:remove(Seq, PendingAcks));
                        _ ->
                            io:format(">>> LEADER ~w: ACK from ~w for seq ~w, waiting for ~w more~n", 
                                    [Id, From, Seq, length(NewRemaining)]),
                            leader(Id, Master, W, Slaves, Group, 
                                   PendingAcks#{Seq := {Message, NewRemaining, RetryCount}})
                    end;
                _ ->
                    leader(Id, Master, W, Slaves, Group, PendingAcks)
            end;
        
        {join, Wrk, Peer} ->
            io:format(">>> LEADER ~w: Adding new node ~w to group~n", [Id, Peer]),
            Slaves2 = Slaves ++ [Peer],
            Group2 = Group ++ [Wrk],
            ViewMessage = {view, W, [self()|Slaves2], Group2},
            bcast_reliable(ViewMessage, Slaves2, PendingAcks, W),
            Master ! {view, Group2},
            leader(Id, Master, W+1, Slaves2, Group2, 
                   PendingAcks#{W => {ViewMessage, Slaves2, 0}});
        
        {retransmit, Seq} ->
            case PendingAcks of
                #{Seq := {Message, RemainingSlaves, RetryCount}} when RetryCount < 3 ->
                    io:format(">>> LEADER ~w: Retransmitting seq ~w (attempt ~w) to ~w nodes~n", 
                             [Id, Seq, RetryCount + 1, length(RemainingSlaves)]),
                    lists:foreach(fun(Slave) -> Slave ! Message end, RemainingSlaves),
                    erlang:send_after(500, self(), {retransmit, Seq}),
                    leader(Id, Master, W, Slaves, Group, 
                           PendingAcks#{Seq := {Message, RemainingSlaves, RetryCount + 1}});
                #{Seq := {_Message, RemainingSlaves, _RetryCount}} ->
                    io:format("!!! LEADER ~w: MAX RETRIES for seq ~w, ~w nodes still missing~n", 
                             [Id, Seq, length(RemainingSlaves)]),
                    leader(Id, Master, W, Slaves, Group, maps:remove(Seq, PendingAcks));
                _ ->
                    leader(Id, Master, W, Slaves, Group, PendingAcks)
            end;
        
        heartbeat ->
            case Slaves of
                [] -> 
                    ok;
                _ -> 
                    bcast_reliable({heartbeat, W}, Slaves, PendingAcks, W)
            end,
            erlang:send_after(300, self(), heartbeat),
            leader(Id, Master, W, Slaves, Group, PendingAcks);
        
        {'EXIT', _Pid, _Reason} -> ok;
        stop -> 
            io:format(">>> LEADER ~w: Received stop command~n", [Id]),
            ok;
        Unexpected ->
            io:format("??? LEADER ~w: Unexpected message: ~w~n", [Id, Unexpected]),
            leader(Id, Master, W, Slaves, Group, PendingAcks)
    end.

%% Slave process with strategic logging
slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, PendingMsgs) ->
    receive
        {mcast, Msg} ->
            io:format(">>> SLAVE ~w: Forwarding message to leader ~w~n", [Id, Leader]),
            reliable_send(Leader, {mcast, Msg}),
            slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, PendingMsgs);
        
        {join, Wrk, Peer} ->
            reliable_send(Leader, {join, Wrk, Peer}),
            slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, PendingMsgs);
        
        {msg, I, Msg} ->
            if 
                I < W ->
                    io:format(">>> SLAVE ~w: Duplicate message ~w (already at ~w), sending ACK~n", [Id, I, W]),
                    reliable_send(Leader, {ack, self(), I}),
                    slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, PendingMsgs);
                I == W ->
                    io:format(">>> SLAVE ~w: Processing message ~w, sending ACK~n", [Id, I]),
                    Master ! Msg,
                    reliable_send(Leader, {ack, self(), I}),
                    slave(Id, Master, Leader, MonitorRef, W+1, {msg, I, Msg}, % Store last processed message
                          Slaves, Group, PendingMsgs);
                I > W ->
                    io:format(">>> SLAVE ~w: Future message ~w (expected ~w), storing~n", [Id, I, W]),
                    NewPending = PendingMsgs#{I => {msg, I, Msg}},
                    slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, NewPending)
            end;
        

        {view, I, [NewLeader|Slaves2], Group2} ->
            if 
                I < W ->
                    reliable_send(Leader, {ack, self(), I}),
                    slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, PendingMsgs);
                I == W ->
                    io:format(">>> SLAVE ~w: Updating view, new leader: ~w~n", [Id, NewLeader]),
                    Master ! {view, Group2},
                    reliable_send(Leader, {ack, self(), I}),
                    erlang:demonitor(MonitorRef, [flush]),
                    NewMonitorRef = erlang:monitor(process, NewLeader),
                    slave(Id, Master, NewLeader, NewMonitorRef, W+1, 
                          {view, I, [NewLeader|Slaves2], Group2}, Slaves2, Group2, PendingMsgs);
                I > W ->
                    NewPending = PendingMsgs#{I => {view, I, [NewLeader|Slaves2], Group2}},
                    slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, NewPending)
            end;
        
        {heartbeat, Seq} ->
            reliable_send(Leader, {ack, self(), Seq}),
            slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, PendingMsgs);
        
        {'DOWN', MonitorRef, process, Leader, Reason} ->
            io:format("!!! SLAVE ~w: LEADER ~w IS DOWN! Reason: ~w~n", [Id, Leader, Reason]),
            io:format("!!! SLAVE ~w: Starting election process...~n", [Id]),
            reliable_election(Id, Master, W, Last, Slaves, Group, PendingMsgs);
        
        {retry, Msg} ->
            io:format(">>> SLAVE ~w: Retrying message to leader~n", [Id]),
            reliable_send(Leader, Msg),
            slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, PendingMsgs);
        
        {'EXIT', _Pid, _Reason} -> ok;
        stop -> 
            io:format(">>> SLAVE ~w: Received stop command~n", [Id]),
            ok;
        Unexpected ->
            io:format("??? SLAVE ~w: Unexpected message: ~w~n", [Id, Unexpected]),
            slave(Id, Master, Leader, MonitorRef, W, Last, Slaves, Group, PendingMsgs)
    end.

%% Election with detailed logging
reliable_election(Id, Master, W, Last, Slaves, Group, PendingMsgs) ->
    Self = self(),
    io:format("*** ELECTION: Node ~w checking if it should become leader~n", [Id]),
    io:format("*** ELECTION: Current slaves list: ~w~n", [Slaves]),
    case Slaves of
        [Self|Rest] ->
            io:format("*** ELECTION: Node ~w BECOMING NEW LEADER!~n", [Id]),
            io:format("*** ELECTION: Resending last message to ensure no loss~n"),
            self() ! heartbeat,
            bcast_reliable(Last, Rest, #{}, W),  %% Resend last message to slaves
            bcast_reliable({view, W, [Self|Rest], Group}, Rest, #{}, W),
            Master ! {view, Group},
            io:format("*** ELECTION: Node ~w transitioned to LEADER role~n", [Id]),
            leader(Id, Master, W+1, Rest, Group, #{});
        [NewLeader|Rest] ->
            io:format("*** ELECTION: Node ~w electing ~w as new leader~n", [Id, NewLeader]),
            MonitorRef = erlang:monitor(process, NewLeader),
            slave(Id, Master, NewLeader, MonitorRef, W, Last, Rest, Group, PendingMsgs)
    end.

%% Broadcast helper
bcast_reliable(Msg, Nodes, PendingAcks, Seq) ->
    io:format(">>> BROADCAST: Sending to ~w nodes: ~w~n", [length(Nodes), element(1, Msg)]),
    lists:foreach(fun(Node) -> Node ! Msg end, Nodes),
    case Msg of
        {msg, S, _} -> erlang:send_after(500, self(), {retransmit, S});
        {view, S, _, _} -> erlang:send_after(500, self(), {retransmit, S});
        _ -> ok
    end.

%% Reliable send helper
reliable_send(Dest, Msg) ->
    Dest ! Msg,
    case Msg of
        {mcast, _} -> erlang:send_after(500, self(), {retry, Msg});
        {join, _, _} -> erlang:send_after(500, self(), {retry, Msg});
        _ -> ok
    end.

%% Stop a node
stop(Node) ->
    io:format(">>> Sending stop command to node ~w~n", [Node]),
    Node ! stop.
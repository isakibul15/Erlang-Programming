%%% ------------------------------------------------------------------
%%% gms4: Atomic multicast with NACK-based loss recovery (optional task)
%%% ------------------------------------------------------------------
-module(gms4).
-export([start/1, start/2]).

-define(HIST, 512).
-define(JOIN_TIMEOUT, 5000).

%%% ========== Public API ==========
start(Id) ->
    Self = self(),
    {ok, spawn_link(fun() -> init_first(Id, Self) end)}.

start(Id, Grp) ->
    Self = self(),
    {ok, spawn_link(fun() -> init_join(Id, Grp, Self) end)}.

%%% ========== First node ==========
init_first(Id, Master) ->
    %% Leader state: Next, Slaves, Group, HQ/HM, Lossy, DropPct
    Next = 1,
    HQ = queue:new(),
    HM = #{},
    Lossy = false,
    DropPct = 0,
    leader(Id, Master, Next, [], [Master], HQ, HM, Lossy, DropPct).

%%% ========== Join path ==========
init_join(Id, Grp, Master) ->
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view, Seq, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            Mon = erlang:monitor(process, Leader),
            Expected = Seq + 1,
            Last = {view, Seq, [Leader|Slaves], Group},
            Pending = #{},
            HQ = queue:from_list([Seq]),
            HM = #{ Seq => Last },
            %% carry Lossy flags in slave state for test convenience
            Lossy = false, DropPct = 0,
            slave(Id, Master, Leader, Mon, Expected, Last,
                  Slaves, Group, Pending, HQ, HM, Lossy, DropPct)
    after ?JOIN_TIMEOUT ->
        Master ! {error, "no reply from leader"}
    end.

%%% ========== Leader ==========
leader(Id, Master, Next, Slaves, Group, HQ, HM, Lossy, DropPct) ->
    receive
        %% Control: enable/disable lossy multicast (test hook)
        {mcast, {ctl_set_lossy, Enable, Pct}} when is_boolean(Enable), is_integer(Pct) ->
            io:format("leader ~w: lossy=~p drop=~p%~n", [Id, Enable, Pct]),
            leader(Id, Master, Next, Slaves, Group, HQ, HM, Enable, Pct);

        %% App/peer multicast
        {mcast, Msg} ->
            M = {msg, Next, Msg},
            bcast(M, Slaves, Lossy, DropPct),
            Master ! Msg,
            {HQ2, HM2} = hist_put(Next, M, HQ, HM),
            leader(Id, Master, Next+1, Slaves, Group, HQ2, HM2, Lossy, DropPct);

        %% Join request
        {join, Wrk, Peer} ->
            Slaves2 = Slaves ++ [Peer],
            Group2  = Group  ++ [Wrk],
            V = {view, Next, [self()|Slaves2], Group2},
            bcast(V, Slaves2, Lossy, DropPct),
            Master ! {view, Group2},
            {HQ2, HM2} = hist_put(Next, V, HQ, HM),
            leader(Id, Master, Next+1, Slaves2, Group2, HQ2, HM2, Lossy, DropPct);

        %% NACK: resend reliably from history
        {nack, From, FromSeq, ToSeq} when FromSeq =< ToSeq ->
            resend_range(From, FromSeq, ToSeq, HM),
            leader(Id, Master, Next, Slaves, Group, HQ, HM, Lossy, DropPct);

        stop -> ok;

        Unexpected ->
            io:format("leader ~w: unexpected ~p~n", [Id, Unexpected]),
            leader(Id, Master, Next, Slaves, Group, HQ, HM, Lossy, DropPct)
    end.

%%% ========== Slave ==========
slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group,
      Pending, HQ, HM, Lossy, DropPct) ->
    receive
        %% Forward app multicasts
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group, Pending, HQ, HM, Lossy, DropPct);

        %% Allow test to toggle lossy via multicast control
        {send, {ctl_set_lossy, Enable, Pct}} ->
            %% Use worker's send->mcast path; just forward a multicast control
            Leader ! {mcast, {ctl_set_lossy, Enable, Pct}},
            slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group, Pending, HQ, HM, Enable, Pct);

        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group, Pending, HQ, HM, Lossy, DropPct);

        %% Numbered regular messages
        {msg, I, _} when I < Expected ->
            %% duplicate, ignore
            slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group, Pending, HQ, HM, Lossy, DropPct);

        {msg, I, Msg} when I == Expected ->
            Master ! Msg,
            {Expected2, Last2, Pending2, HQ2, HM2} =
                drain_in_order(Expected+1, {msg, I, Msg}, Pending, HQ, HM, Master),
            slave(Id, Master, Leader, Mon, Expected2, Last2, Slaves, Group, Pending2, HQ2, HM2, Lossy, DropPct);

        {msg, I, Msg} when I > Expected ->
            io:format("slave ~w: gap on msg, expect=~p got=~p -> NACK ~p..~p~n",
                      [Id, Expected, I, Expected, I-1]),
            Pending2 = Pending#{ I => {msg, I, Msg} },
            Leader ! {nack, self(), Expected, I-1},
            slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group, Pending2, HQ, HM, Lossy, DropPct);

        %% Numbered views
        {view, I, [NewLeader|_], _} when I < Expected ->
            slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group, Pending, HQ, HM, Lossy, DropPct);

        {view, I, [NewLeader|Slaves2], Group2} when I == Expected ->
            Master ! {view, Group2},
            erlang:demonitor(Mon, [flush]),
            NewMon = erlang:monitor(process, NewLeader),
            V = {view, I, [NewLeader|Slaves2], Group2},
            {Expected2, Last2, Pending2, HQ2, HM2} =
                drain_in_order(Expected+1, V, Pending, HQ, HM, Master),
            slave(Id, Master, NewLeader, NewMon, Expected2, Last2, Slaves2, Group2, Pending2, HQ2, HM2, Lossy, DropPct);

        {view, I, Peers, Group2} when I > Expected ->
            io:format("slave ~w: gap on view, expect=~p got=~p -> NACK ~p..~p~n",
                      [Id, Expected, I, Expected, I-1]),
            Pending2 = Pending#{ I => {view, I, Peers, Group2} },
            Leader ! {nack, self(), Expected, I-1},
            slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group, Pending2, HQ, HM, Lossy, DropPct);

        %% Leader died: elect
        {'DOWN', Mon, process, Leader, _Reason} ->
            election(Id, Master, Expected, Last, Slaves, Group, Pending, HQ, HM, Lossy, DropPct);

        stop -> ok;

        Unexpected ->
            io:format("slave ~w: unexpected ~p~n", [Id, Unexpected]),
            slave(Id, Master, Leader, Mon, Expected, Last, Slaves, Group, Pending, HQ, HM, Lossy, DropPct)
    end.

%%% ========== Election with history carry-over ==========
election(Id, Master, Expected, Last, Slaves, [_|Group], Pending, HQ, HM, Lossy, DropPct) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
            io:format("Node ~w: I am the new leader (gms4), resending last and serving NACKs~n", [Id]),
            %% close any half-broadcast
            bcast(Last, Rest, false, 0),  %% reliable resend
            Master ! case Last of {view,_,_,G} -> {view, G}; {msg,_,M} -> M end,
            leader(Id, Master, Expected, Rest, Group, HQ, HM, Lossy, DropPct);
        [NewLeader|Rest] ->
            io:format("Node ~w: Electing ~w as new leader (gms4)~n", [Id, NewLeader]),
            Mon = erlang:monitor(process, NewLeader),
            slave(Id, Master, NewLeader, Mon, Expected, Last, Rest, Group, Pending, HQ, HM, Lossy, DropPct)
    end.

%%% ========== Utilities ==========
bcast(Msg, Nodes, Lossy, DropPct) ->
    lists:foreach(
      fun(N) ->
          case Lossy of
              false -> N ! Msg;
              true  ->
                  %% probabilistically drop on a per-destination basis
                  case rand:uniform(100) =< DropPct of
                      true  -> ok;
                      false -> N ! Msg
                  end
          end
      end, Nodes).

resend_range(To, FromSeq, ToSeq, HM) ->
    lists:foreach(
      fun(S) ->
          case maps:get(S, HM, undefined) of
              undefined -> ok;
              M -> To ! M
          end
      end,
      lists:seq(FromSeq, ToSeq)).

hist_put(Seq, M, HQ, HM) ->
    HQ1 = queue:in(Seq, HQ),
    HM1 = HM#{ Seq => M },
    trim_hist(HQ1, HM1).

trim_hist(HQ, HM) ->
    case queue:len(HQ) > ?HIST of
        true ->
            {{value, Old}, HQ2} = queue:out(HQ),
            {HQ2, maps:remove(Old, HM)};
        false ->
            {HQ, HM}
    end.

%% Drain pending consecutive messages starting from ExpectedNext
drain_in_order(ExpectedNext, ThisM, Pending, HQ, HM, Master) ->
    {Exp1, Last1, HQ1, HM1} = deliver_in_order(ThisM, ExpectedNext, HQ, HM, Master),
    drain_loop(Exp1, Last1, Pending, HQ1, HM1, Master).

drain_loop(Expected, Last, Pending, HQ, HM, Master) ->
    case maps:get(Expected, Pending, undefined) of
        undefined ->
            {Expected, Last, Pending, HQ, HM};
        M ->
            Pending2 = maps:remove(Expected, Pending),
            {Exp1, Last1, HQ1, HM1} = deliver_in_order(M, Expected+1, HQ, HM, Master),
            drain_loop(Exp1, Last1, Pending2, HQ1, HM1, Master)
    end.

deliver_in_order(M, NextAfter, HQ, HM, Master) ->
    case M of
        {msg, I, Msg} ->
            Master ! Msg,
            {HQ2, HM2} = hist_put(I, M, HQ, HM),
            {NextAfter, {msg, I, Msg}, HQ2, HM2};
        {view, I, _Peers, Group2} ->
            Master ! {view, Group2},
            {HQ2, HM2} = hist_put(I, M, HQ, HM),
            {NextAfter, {view, I, _Peers, Group2}, HQ2, HM2}
    end.

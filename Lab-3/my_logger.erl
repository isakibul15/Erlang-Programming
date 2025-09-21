-module(my_logger).
-export([start/1, stop/1]).

-record(state, {
    clocks = #{} :: map(),                    %% #{Name => LastSeenLamport}
    hb = []       :: list()                   %% [{Time,From,Msg}, ...] (sorted)
}).

start(Names) ->
    spawn_link(fun() -> init(Names) end).

stop(Logger) ->
    Logger ! stop.

init(Names) ->
    Clocks0 = time:clock(Names),
    loop(#state{clocks = Clocks0, hb = []}).

loop(State = #state{clocks = Clocks, hb = HB}) ->
    receive
        {log, From, Time, Msg} ->
            %% 1) Update per-node clocks
            Clocks1 = time:update(From, Time, Clocks),
            %% 2) Insert into hold-back queue (ordered by {Time,From})
            HB1 = insert({Time, From, Msg}, HB),
            %% 3) Drain all safe messages
            State1 = drain_safe(#state{clocks = Clocks1, hb = HB1}),
            loop(State1);

        stop ->
            %% Final drain: at shutdown, print whatever is left in total order.
            %% (No more arrivals; it's safe to flush.)
            flush_all(State),
            ok
    end.

%% Insert and keep the queue ordered by {Time,From}
insert(Item = {T, F, _}, HB) ->
    lists:sort(fun({T1,F1,_},{T2,F2,_}) ->
                       case T1 =:= T2 of
                           true  -> F1 =< F2;   %% tie-breaker by sender
                           false -> T1  =< T2
                       end
               end, [Item | HB]).

%% Try to print as long as the smallest item is safe
drain_safe(State = #state{clocks = Clocks, hb = []}) ->
    State;
drain_safe(State = #state{clocks = Clocks, hb = [{T,From,Msg}|Rest]}) ->
    case time:safe(T, Clocks) of
        true ->
            do_print(T, From, Msg),
            %% keep draining
            drain_safe(State#state{hb = Rest});
        false ->
            State
    end.

do_print(Time, From, Msg) ->
    io:format("log: ~p ~p ~p~n", [Time, From, Msg]),
    %% test hook (no-op unless a process is registered as log_tap)
    case whereis(log_tap) of
        undefined -> ok;
        Tap -> Tap ! {printed, Time, From, Msg}
    end.

flush_all(#state{hb = HB}) ->
    lists:foreach(fun({T,F,M}) -> do_print(T,F,M) end, HB).

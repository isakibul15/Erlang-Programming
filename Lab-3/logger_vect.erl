-module(logger_vect).
-export([start/1, stop/1]).

start(Nodes) ->
    spawn_link(fun() -> init(Nodes) end).

stop(Logger) ->
    Logger ! stop.

init(Nodes) ->
    Clock = vect:clock(Nodes),
    loop(Clock, []).

loop(Clock, Queue) ->
    receive
        {log, From, Time, Msg} ->
            NewClock = vect:update(From, Time, Clock),
            NewQueue = insert({Time, From, Msg}, Queue),
            {SafeMessages, UpdatedQueue} = process_queue(NewClock, NewQueue, []),
            lists:foreach(fun({T, F, M}) -> log(F, T, M) end, SafeMessages),
            loop(NewClock, UpdatedQueue);
        stop ->
            ok
    end.

%% Insert message into queue maintaining vector time order
insert(Message, []) ->
    [Message];
insert({T1, From1, Msg1}, [{T2, From2, Msg2} | Rest]) ->
    case vect:leq(T1, T2) of
        true ->
            [{T1, From1, Msg1}, {T2, From2, Msg2} | Rest];
        false ->
            [{T2, From2, Msg2} | insert({T1, From1, Msg1}, Rest)]
    end.

process_queue(Clock, [], Acc) ->
    {lists:reverse(Acc), []};
process_queue(Clock, [{Time, From, Msg} | Rest], Acc) ->
    case vect:safe(Time, Clock) of
        true ->
            process_queue(Clock, Rest, [{Time, From, Msg} | Acc]);
        false ->
            {lists:reverse(Acc), [{Time, From, Msg} | Rest]}
    end.

log(From, Time, Msg) ->
    io:format("log: ~w from ~w: ~p~n", [Time, From, Msg]).
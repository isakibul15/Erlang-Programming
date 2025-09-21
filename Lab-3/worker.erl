-module(worker).
-export([start/5, stop/1, peers/2]).

start(Name, Logger, Seed, Sleep, Jitter) ->
    spawn_link(fun() -> init(Name, Logger, Seed, Sleep, Jitter) end).

stop(Worker) ->
    Worker ! stop.

peers(Wrk, Peers) ->
    Wrk ! {peers, Peers}.

init(Name, Log, Seed, Sleep, Jitter) ->
    rand:seed(exsss, {Seed, Seed, Seed}),
    receive
        {peers, Peers} ->
            loop(Name, Log, Peers, Sleep, Jitter, time:zero());
        stop ->
            ok
    end.

loop(Name, Log, Peers, Sleep, Jitter, Time) ->
    Wait = rand:uniform(Sleep),
    receive
        {msg, From, TimeMsg, Msg} ->
            MergedTime = time:merge(Time, TimeMsg),
            NewTime = time:inc(Name, MergedTime),
            Log ! {log, Name, NewTime, {received, Msg}},
            loop(Name, Log, Peers, Sleep, Jitter, NewTime);
        stop ->
            ok;
        Error ->
            Log ! {log, Name, Time, {error, Error}},
            loop(Name, Log, Peers, Sleep, Jitter, Time)
    after Wait ->
        Selected = select(Peers),
        NewTime = time:inc(Name, Time),
        Message = {hello, rand:uniform(100)},
        Selected ! {msg, Name, NewTime, Message},
        jitter(Jitter),
        Log ! {log, Name, NewTime, {sending, Message}},
        loop(Name, Log, Peers, Sleep, Jitter, NewTime)
    end.

select(Peers) ->
    lists:nth(rand:uniform(length(Peers)), Peers).

jitter(0) -> ok;
jitter(Jitter) -> timer:sleep(rand:uniform(Jitter)).
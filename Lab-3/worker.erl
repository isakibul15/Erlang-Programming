-module(worker).
-export([start/5, stop/1, peers/2]).

start(Name, Logger, Seed, Sleep, Jitter) ->
    spawn_link(fun() -> init(Name, Logger, Seed, Sleep, Jitter) end).

stop(Worker) ->
    Worker ! stop.

peers(Wrk, Peers) ->
    Wrk ! {peers, Peers}.

init(Name, Log, Seed, Sleep, Jitter) ->
    % Replace deprecated random:seed with rand:seed
    rand:seed(exsss, {Seed, Seed, Seed}),
    receive
        {peers, Peers} ->
            loop(Name, Log, Peers, Sleep, Jitter);
        stop ->
            ok
    end.

loop(Name, Log, Peers, Sleep, Jitter) ->
    % Replace deprecated random:uniform with rand:uniform
    Wait = rand:uniform(Sleep),
    receive
        {msg, Time, Msg} ->
            Log ! {log, Name, Time, {received, Msg}},
            loop(Name, Log, Peers, Sleep, Jitter);
        stop ->
            ok;
        Error ->
            Log ! {log, Name, na, {error, Error}}
    after Wait ->
        Selected = select(Peers),
        Time = na,
        % Replace deprecated random:uniform with rand:uniform
        Message = {hello, rand:uniform(100)},
        Selected ! {msg, Time, Message},
        jitter(Jitter),
        Log ! {log, Name, Time, {sending, Message}},
        loop(Name, Log, Peers, Sleep, Jitter)
    end.

select(Peers) ->
    % Replace deprecated random:uniform with rand:uniform
    lists:nth(rand:uniform(length(Peers)), Peers).

jitter(0) -> ok;
% Replace deprecated random:uniform with rand:uniform
jitter(Jitter) -> timer:sleep(rand:uniform(Jitter)).
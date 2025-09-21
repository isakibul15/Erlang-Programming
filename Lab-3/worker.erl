-module(worker).
-export([start/5, stop/1, peers/2]).

start(Name, Logger, Seed, Sleep, Jitter) ->
    spawn_link(fun() -> init(Name, Logger, Seed, Sleep, Jitter) end).

stop(Worker) ->
    Worker ! stop.

peers(Wrk, Peers) ->
    Wrk ! {peers, Peers}.

init(Name, Log, Seed, Sleep, Jitter) ->
    %% Proper rand seeding
    rand:seed(exsplus, {Seed, Seed*2+1, Seed*3+7}),
    receive
        {peers, Peers} ->
            T0 = time:zero(),
            loop(Name, Log, Peers, Sleep, Jitter, T0);
        stop ->
            ok
    end.

loop(Name, Log, Peers, Sleep, Jitter, T) ->
    Wait = rand:uniform(Sleep),
    receive
        {msg, Tm, Msg} ->
            %% Merge then inc on receive
            T1 = time:merge(T, Tm),
            T2 = time:inc(T1, Name),
            Log ! {log, Name, T2, {received, Msg}},
            loop(Name, Log, Peers, Sleep, Jitter, T2);

        stop ->
            ok;

        Error ->
            %% Keep running after logging the error
            Log ! {log, Name, T, {error, Error}},
            loop(Name, Log, Peers, Sleep, Jitter, T)
    after Wait ->
        Selected = select(Peers),
        %% Increment on local send event
        T1 = time:inc(T, Name),
        Message = {hello, rand:uniform(100)},
        Selected ! {msg, T1, Message},
        jitter(Jitter),
        Log ! {log, Name, T1, {sending, Message}},
        loop(Name, Log, Peers, Sleep, Jitter, T1)
    end.

select([]) ->
    exit(no_peers);
select(Peers) ->
    lists:nth(rand:uniform(length(Peers)), Peers).

jitter(0) ->
    ok;
jitter(Jitter) ->
    timer:sleep(rand:uniform(Jitter)).

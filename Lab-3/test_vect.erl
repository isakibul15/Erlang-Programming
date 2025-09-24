-module(test_vect).
-export([run/2]).

run(Sleep, Jitter) ->
    Log = logger_vect:start([john, paul, ringo, george]),
    A = worker_vect:start(john, Log, 13, Sleep, Jitter),
    B = worker_vect:start(paul, Log, 23, Sleep, Jitter),
    C = worker_vect:start(ringo, Log, 36, Sleep, Jitter),
    D = worker_vect:start(george, Log, 49, Sleep, Jitter),
    worker_vect:peers(A, [B, C, D]),
    worker_vect:peers(B, [A, C, D]),
    worker_vect:peers(C, [A, B, D]),
    worker_vect:peers(D, [A, B, C]),
    timer:sleep(5000),
    logger_vect:stop(Log),
    worker_vect:stop(A),
    worker_vect:stop(B),
    worker_vect:stop(C),
    worker_vect:stop(D).
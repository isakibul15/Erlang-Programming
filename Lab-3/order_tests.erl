%% =========================================================
%% order_tests.erl — causal-order checks (assignment-accurate)
%% Run: c(order_tests), order_tests:all().
%% =========================================================
-module(order_tests).
-export([all/0]).

assert(true) -> ok;
assert(false) -> exit({assertion_failed});
assert(Other) -> exit({assertion_failed, Other}).

all() ->
    io:format("~n-- order_tests: starting --~n", []),
    with_tap(fun() -> run_cases() end).

run_cases() ->
    {Log, Pids} = start_system(50, 20),
    Prints = collect_prints(200, 4000),
    stop_system(Log, Pids),
    assert(nondecreasing_time(Prints)),
    assert(sending_before_receiving(Prints)),
    io:format("Case 1 OK (~p prints)~n", [length(Prints)]),

    {Log2, Pids2} = start_system(80, 0),
    Prints2 = collect_prints(150, 4000),
    stop_system(Log2, Pids2),
    assert(nondecreasing_time(Prints2)),
    assert(sending_before_receiving(Prints2)),
    io:format("Case 2 OK (~p prints)~n", [length(Prints2)]),

    {Log3, Pids3} = start_system(100, 50),
    Prints3 = collect_prints(200, 5000),
    stop_system(Log3, Pids3),
    assert(nondecreasing_time(Prints3)),
    assert(sending_before_receiving(Prints3)),
    io:format("Case 3 OK (~p prints)~n", [length(Prints3)]),
    ok.

%% --- Spin-up/tear-down identical to before ---
start_system(Sleep, Jitter) ->
    Log = my_logger:start([john, paul, ringo, george]),
    A = worker:start(john,  Log, 13, Sleep, Jitter),
    B = worker:start(paul,  Log, 23, Sleep, Jitter),
    C = worker:start(ringo, Log, 36, Sleep, Jitter),
    D = worker:start(george,Log, 49, Sleep, Jitter),
    worker:peers(A, [B,C,D]),
    worker:peers(B, [A,C,D]),
    worker:peers(C, [A,B,D]),
    worker:peers(D, [A,B,C]),
    {Log,{A,B,C,D}}.

stop_system(Log, {A,B,C,D}) ->
    my_logger:stop(Log),
    worker:stop(A), worker:stop(B), worker:stop(C), worker:stop(D),
    ok.

with_tap(Fun) ->
    Self = self(),
    register(log_tap, Self),
    try Fun()
    after catch unregister(log_tap) end.

collect_prints(N, Timeout) -> collect_prints(N, Timeout, []).
collect_prints(0, _Timeout, Acc) -> lists:reverse(Acc);
collect_prints(N, Timeout, Acc) ->
    receive
        {printed, T, From, Msg} ->
            collect_prints(N-1, Timeout, [{T,From,Msg}|Acc])
    after Timeout ->
        lists:reverse(Acc)
    end.

%% --- Properties ---

%% Only require Lamport time to be nondecreasing globally.
nondecreasing_time([{T1,_,_},{T2,_,_}|Rest]) when T1 =< T2 ->
    nondecreasing_time([{T2,ignored,ignored}|Rest]);
nondecreasing_time([_]) -> true;
nondecreasing_time([])  -> true;
nondecreasing_time(_)   -> false.

%% For each Msg, a 'sending' must be seen before any 'received' of same Msg.
sending_before_receiving(Prints) ->
    Seen = dict:new(),
    sending_before_receiving(Prints, Seen).

sending_before_receiving([], _Seen) -> true;
sending_before_receiving([{_T, _From, {sending, Msg}}|Rest], Seen) ->
    sending_before_receiving(Rest, dict:store(Msg, true, Seen));
sending_before_receiving([{_T, _From, {received, Msg}}|Rest], Seen) ->
    case dict:is_key(Msg, Seen) of
        true  -> sending_before_receiving(Rest, Seen);
        false -> false
    end;
sending_before_receiving([_|Rest], Seen) ->
    sending_before_receiving(Rest, Seen).

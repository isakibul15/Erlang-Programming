-module(time).
-export([
    zero/0, inc/2, merge/2, leq/2,
    clock/1,               %% make per-node clock map
    update/3,              %% update(Name, Time, ClockMap) -> ClockMap'
    safe/2                 %% safe(Time, ClockMap) -> boolean()
]).

%% ---- Lamport scalar time ----

zero() ->
    0.

inc(T, _Name) when is_integer(T) ->
    T + 1.

merge(T1, T2) when is_integer(T1), is_integer(T2) ->
    if T1 >= T2 -> T1; true -> T2 end.

leq(T1, T2) when is_integer(T1), is_integer(T2) ->
    T1 =< T2.

%% ---- Logger helpers ----

%% clock/1: initialize a map #{Name => 0, ...}
clock(Names) when is_list(Names) ->
    lists:foldl(fun(N, M) -> M#{N => 0} end, #{}, Names).

%% update/3: advance knowledge about Name's clock
update(Name, Time, Clocks) when is_map(Clocks), is_integer(Time) ->
    Prev = maps:get(Name, Clocks, 0),
    Clocks#{Name => (if Time > Prev -> Time; true -> Prev end)}.

%% safe/2: a message with timestamp T is safe to print iff
%% T =< min over all known process times
safe(T, Clocks) when map_size(Clocks) > 0 ->
    Min = lists:min(maps:values(Clocks)),
    T =< Min.
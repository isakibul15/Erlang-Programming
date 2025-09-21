-module(time).
-export([zero/0, inc/2, merge/2, leq/2]).

%% Start from zero
zero() ->
    0.

%% Increment local clock (Name is unused in scalar Lamport, but kept for API parity)
inc(T, _Name) when is_integer(T) ->
    T + 1.

%% Merge two Lamport times (take the max)
merge(T1, T2) when is_integer(T1), is_integer(T2) ->
    if T1 >= T2 -> T1; true -> T2 end.

%% Less-or-equal (useful later)
leq(T1, T2) when is_integer(T1), is_integer(T2) ->
    T1 =< T2.
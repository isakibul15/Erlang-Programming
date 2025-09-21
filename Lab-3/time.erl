-module(time).
-export([zero/0, inc/2, merge/2, leq/2, clock/1, update/3, safe/2]).

zero() ->
    0.

inc(_Name, T) ->
    T + 1.

merge(Ti, Tj) ->
    max(Ti, Tj).

leq(Ti, Tj) ->
    Ti =< Tj.

clock(Nodes) ->
    lists:foldl(fun(Node, Acc) -> maps:put(Node, 0, Acc) end, #{}, Nodes).

update(Node, Time, Clock) ->
    Current = maps:get(Node, Clock, 0),
    NewTime = max(Current, Time),
    maps:put(Node, NewTime, Clock).

safe(Time, Clock) ->
    lists:all(fun({_Node, T}) -> T >= Time end, maps:to_list(Clock)).
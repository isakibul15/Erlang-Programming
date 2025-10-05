
-module(key).
-export([generate/0, between/3]).

%% @doc Generate a random 30-bit key in [1, 1000000000].
generate() ->
    random:uniform(1000000000).

%% @doc Return true if K is in the ring interval (From, To], inclusive of To.
%% Wrap-around is handled. If From == To, the interval is the full circle.
between(K, From, To) when From == To ->
    true;
between(K, From, To) when From < To ->
    (K > From) andalso (K =< To);
between(K, From, To) ->
    (K > From) orelse (K =< To).

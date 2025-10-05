-module(storage).
-export([create/0, add/3, lookup/2, split/3, merge/2]).

create() ->
    [].

add(K, V, Store) ->
    [{K,V} | lists:keydelete(K, 1, Store)].

lookup(K, Store) ->
    case lists:keyfind(K, 1, Store) of
        false -> false;
        {_, V} = KV -> KV
    end.

%% Keep keys in (From, To], return {Kept, Rest}
split(From, To, Store) ->
    lists:partition(fun({K,_}) -> key:between(K, From, To) end, Store).

merge(Entries, Store) ->
    lists:foldl(fun({K,V}, S) -> add(K, V, S) end, Store, Entries).

-module(hist).
-export([new/1, update/3]).

new(Name) ->
    [{Name, inf}].

update(Node, N, History) ->
    case lists:keyfind(Node, 1, History) of
        false ->
            {new, [{Node, N} | History]};
        {Node, Current} ->
            if
                N > Current ->
                    Updated = lists:keyreplace(Node, 1, History, {Node, N}),
                    {new, Updated};
                true ->
                    old
            end
    end.
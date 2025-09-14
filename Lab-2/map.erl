-module(map).
-export([new/0, update/3, reachable/2, all_nodes/1]).

new() ->
    [].

update(Node, Links, Map) ->
    % Remove any existing entry for Node
    FilteredMap = lists:keydelete(Node, 1, Map),
    % Add the new entry
    [{Node, Links} | FilteredMap].

reachable(Node, Map) ->
    case lists:keyfind(Node, 1, Map) of
        {Node, Links} -> Links;
        false -> []
    end.

all_nodes(Map) ->
    % Extract all nodes that are keys in the map
    Keys = [Node || {Node, _} <- Map],
    % Extract all nodes from all links
    LinkedNodes = lists:flatmap(fun({_, Links}) -> Links end, Map),
    % Combine and remove duplicates
    lists:usort(Keys ++ LinkedNodes).
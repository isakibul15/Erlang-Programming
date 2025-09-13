-module(dijkstra).
% -export([table/2, route/2, update/4]).
% -export([table/2, route/2, iterate/3]).
-export([table/2, route/2]).

entry(Node, Sorted) ->
    case lists:keyfind(Node, 1, Sorted) of
        {Node, Length, _} -> Length;
        false -> 0
    end.

replace(Node, N, Gateway, Sorted) ->
    Removed = lists:keydelete(Node, 1, Sorted),
    Updated = [{Node, N, Gateway} | Removed],
    lists:keysort(2, Updated).

update(Node, N, Gateway, Sorted) ->
    Current = entry(Node, Sorted),
    if 
        N < Current -> replace(Node, N, Gateway, Sorted);
        true -> Sorted
    end.

iterate([], _, Table) -> Table;
iterate([{_, inf, _} | _], _, Table) -> Table;
iterate([{Node, Length, Gateway} | Rest], Map, Table) ->
    Reachable = map:reachable(Node, Map),
    UpdatedRest = lists:foldl(fun(Next, Acc) -> 
        update(Next, Length + 1, Gateway, Acc)
    end, Rest, Reachable),
    iterate(UpdatedRest, Map, [{Node, Gateway} | Table]).

table(Gateways, Map) ->
    Nodes = map:all_nodes(Map),
    Initial = lists:map(fun(Node) ->
        case lists:member(Node, Gateways) of
            true -> {Node, 0, Node};
            false -> {Node, inf, unknown}
        end
    end, Nodes),
    Sorted = lists:keysort(2, Initial),
    iterate(Sorted, Map, []).

route(Node, Table) ->
    case lists:keyfind(Node, 1, Table) of
        {Node, Gateway} -> {ok, Gateway};
        false -> notfound
    end.
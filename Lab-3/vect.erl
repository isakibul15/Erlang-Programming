-module(vect).
-export([zero/0, inc/2, merge/2, leq/2, clock/1, update/3, safe/2]).

%% Returns an empty list as the zero vector clock
zero() -> 
    [].

%% Increment the counter for a specific node
inc(Name, Time) ->
    case lists:keyfind(Name, 1, Time) of
        {Name, Count} ->
            lists:keyreplace(Name, 1, Time, {Name, Count + 1});
        false ->
            [{Name, 1} | Time]
    end.

%% Merge two vector clocks by taking the maximum for each node
merge([], Time) -> 
    Time;
merge([{Name, Ti} | Rest], Time) ->
    case lists:keyfind(Name, 1, Time) of
        {Name, Tj} ->
            Max = max(Ti, Tj),
            NewTime = lists:keyreplace(Name, 1, Time, {Name, Max}),
            merge(Rest, NewTime);
        false ->
            merge(Rest, [{Name, Ti} | Time])
    end.

%% Check if Ti <= Tj (Ti is less than or equal to Tj)
leq([], _) -> 
    true;
leq([{Name, Ti} | Rest], Time) ->
    case lists:keyfind(Name, 1, Time) of
        {Name, Tj} ->
            if 
                Ti =< Tj ->
                    leq(Rest, Time);
                true ->
                    false
            end;
        false ->
            %% If node not found in Time, it means Tj = 0, so Ti must be 0
            Ti =< 0 andalso leq(Rest, Time)
    end.

%% Create an initial clock for the given nodes
clock(Nodes) ->
    lists:map(fun(Node) -> {Node, 0} end, Nodes).

%% Update the clock with a new message from a node
update(From, Time, Clock) ->
    case lists:keyfind(From, 1, Time) of
        {From, NewTime} ->
            case lists:keyfind(From, 1, Clock) of
                {From, CurrentTime} ->
                    MaxTime = max(CurrentTime, NewTime),
                    lists:keyreplace(From, 1, Clock, {From, MaxTime});
                false ->
                    [{From, NewTime} | Clock]
            end;
        false ->
            Clock  % If Time doesn't have From, no update needed
    end.

%% Check if it's safe to log a message with the given vector time
safe(Time, Clock) ->
    %% For each entry in Time, check if Clock has at least that value
    lists:all(fun({Node, T}) ->
        case lists:keyfind(Node, 1, Clock) of
            {Node, C} -> T =< C;
            false -> T =< 0  % Missing node implies value 0
        end
    end, Time).
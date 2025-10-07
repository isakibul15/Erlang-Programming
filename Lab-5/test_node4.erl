%%% test_node4.erl — replication survives owner failure (robust)
-module(test_node4).
-export([run/0, kill/1]).

-define(TICK_MS, 400).
-define(RETRIES, 25).   %% ~10s total

run() ->
    timer:start(),
    io:format("Starting ring...~n", []),
    A = node4:start(key:generate()),
    B = node4:start(key:generate(), A),
    C = node4:start(key:generate(), B),

    %% let ring form
    timer:sleep(5000),
    node4:probe(A),

    %% register entry for clients (optional)
    catch global:unregister_name(chord_entry),
    ok = (catch global:register_name(chord_entry, A)),
    AId = get_id(A),
    K   = AId,

    io:format("Adding replicated key ~p via A~n", [K]),
    ok = node4:add(A, K, <<"replicated">>),

    %% allow at least one replication tick
    timer:sleep(6000),

    io:format("Lookup via C (before failure)...~n", []),
    {K,<<"replicated">>} = must_lookup(C, K),

    %% kill owner A
    io:format("Killing node A (owner)...~n", []),
    kill(A),

    %% rebind entry to a live node
    catch global:unregister_name(chord_entry),
    case is_process_alive(B) of
        true  -> ok = (catch global:register_name(chord_entry, B));
        false -> ok = (catch global:register_name(chord_entry, C))
    end,

    %% force healing: ping stabilize a few times
    force_stabilize([B,C], ?RETRIES, ?TICK_MS),

    io:format("Probing ring after failure...~n", []),
    node4:probe(case is_process_alive(B) of true -> B; false -> C end),

    io:format("Lookup via B after owner death...~n", []),
    {K,<<"replicated">>} = must_lookup(case is_process_alive(B) of true -> B; false -> C end, K),

    io:format("Replication survived owner failure.~n", []),
    ok.

kill(Pid) -> exit(Pid, kill).

%% ---- helpers ----------------------------------------------------

get_id(Pid) ->
    Pid ! {print, self()},
    receive
        {state, Id, _Pred, _Succ, _Next, _Succ2, _Store} -> Id
    after 2000 -> exit(timeout_get_id)
    end.

force_stabilize(_Ps, 0, _T) -> ok;
force_stabilize(Ps, N, T) ->
    lists:foreach(fun(P) -> catch (P ! stabilize) end, Ps),
    timer:sleep(T),
    force_stabilize(Ps, N-1, T).

must_lookup(Pid, Key) ->
    try_lookup(Pid, Key, ?RETRIES).

try_lookup(_Pid, _Key, 0) -> exit(timeout_lookup_after_failure);
try_lookup(Pid, Key, N) ->
    case node4:lookup(Pid, Key) of
        {Key, _}=Hit -> Hit;
        _Other ->
            %% nudge stabilize and retry with short delay
            catch (Pid ! stabilize),
            timer:sleep(?TICK_MS),
            try_lookup(Pid, Key, N-1)
    end.

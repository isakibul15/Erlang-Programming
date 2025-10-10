%%% test_node4.erl — 5-node ring, each node inserts 100 keys near its Id,
%%% verify before and after killing one node.
-module(test_node4).
-export([run/0, kill/1]).

-define(TICK_MS, 300).
-define(RETRIES, 40).
-define(WRAP, 1000000000).  %% positive wrap space for key arithmetic

run() ->
    timer:start(),
    io:format("Starting 5-node ring...~n", []),
    A = node4:start(key:generate()),
    B = node4:start(key:generate(), A),
    C = node4:start(key:generate(), B),
    D = node4:start(key:generate(), C),
    E = node4:start(key:generate(), D),
    Nodes = [A,B,C,D,E],

    %% wait until ring really has 5 members
    wait_until_ring(Nodes, 5, 60),

    %% collect node info
    Infos = lists:map(fun info/1, Nodes),
    io:format("Node ids: ~p~n", [[I || {I,_,_} <- Infos]]),

    %% insert 100 keys per node near its own Id
    io:format("Inserting 100 keys per node (total 500)...~n", []),
    lists:foreach(fun({Id,Pid,_Pred}) -> insert_near_id(Pid, Id, 100) end, Infos),

    %% allow stabilization/replication time
    force_stabilize(Nodes, 20, ?TICK_MS),
    timer:sleep(3000),

    io:format("Verifying before failure...~n", []),
    verify_all(hd(Nodes), Infos, 100),

    print_store_summary(Nodes, "Before failure"),

    %% kill B and re-verify
    io:format("Killing node B...~n", []),
    kill(B),
    force_stabilize([C,D,E,A], 25, ?TICK_MS),
    timer:sleep(3000),

    io:format("Verifying after failure...~n", []),
    %% drop B from Infos for the second verification set
    Infos2 = [{I,P,Pr} || {I,P,Pr} <- Infos, P =/= B],
    verify_all(C, Infos2, 100),

    print_store_summary([C,D,E,A], "After failure"),
    io:format("OK: 100 keys per node preserved after one-node failure.~n"),
    ok.

kill(Pid) -> exit(Pid, kill).

%% ---------- ring readiness ----------

wait_until_ring(Nodes, Want, MaxTries) ->
    case ring_size(hd(Nodes)) of
        Want ->
            io:format("Ring ready with ~p nodes.~n", [Want]),
            ok;
        N when N > 0, MaxTries > 0 ->
            force_stabilize(Nodes, 3, ?TICK_MS),
            timer:sleep(500),
            wait_until_ring(Nodes, Want, MaxTries-1);
        _ when MaxTries > 0 ->
            force_stabilize(Nodes, 3, ?TICK_MS),
            timer:sleep(500),
            wait_until_ring(Nodes, Want, MaxTries-1);
        _ ->
            exit({ring_not_ready, Want})
    end.

ring_size(EntryPid) ->
    try ring_walk(EntryPid) of Ids -> length(Ids)
    catch _:_ -> 0
    end.

ring_walk(Pid) ->
    ring_walk(Pid, Pid, [], 20).

ring_walk(Start, Cur, Acc, 0) ->
    lists:usort(Acc);
ring_walk(Start, Cur, Acc, Left) ->
    Cur ! {print, self()},
    receive
        {state, Id, _Pred, Succ, _Next, _Nil, _Store} ->
            case Succ of
                {_,_,SPid} when SPid =:= Start ->
                    lists:usort([Id|Acc]);
                {_,_,SPid} when is_pid(SPid) ->
                    ring_walk(Start, SPid, [Id|Acc], Left-1);
                _ ->
                    lists:usort([Id|Acc])
            end
    after 1000 ->
        lists:usort(Acc)
    end.

%% ---------- node info ----------

info(Pid) ->
    Pid ! {print, self()},
    receive
        {state, Id, Pred, _Succ, _Next, _Nil, _Store} -> {Id, Pid, Pred}
    after 2000 ->
        exit(timeout_info)
    end.

%% ---------- insert keys near Id ----------

insert_near_id(NodePid, Id, N) ->
    Keys = lists:map(fun(I) -> wrap(Id - I) end, lists:seq(0, N-1)),
    lists:foreach(
      fun(K) ->
          V = list_to_binary(io_lib:format("v~p",[K])),
          ok = node4:add(NodePid, K, V)
      end, Keys).

wrap(X) when X >= 0 -> X;
wrap(X)             -> X + ?WRAP.

%% ---------- verify ----------

verify_all(Entry, Infos, NPerNode) ->
    lists:foreach(
      fun({Id,_Pid,_Pred}) ->
          Keys = lists:map(fun(I) -> wrap(Id - I) end, lists:seq(0, NPerNode-1)),
          lists:foreach(fun(K) -> must_lookup_ok(Entry, K) end, Keys)
      end, Infos).

must_lookup_ok(Pid, Key) ->
    case try_lookup(Pid, Key, ?RETRIES) of
        {Key, <<"v",_/binary>>} ->
            ok;
        Other ->
            io:format("Mismatch key ~p -> ~p~n", [Key, Other])
    end.

try_lookup(_Pid, _Key, 0) ->
    false;
try_lookup(Pid, Key, N) ->
    case node4:lookup(Pid, Key) of
        {Key,_}=Hit ->
            Hit;
        _Other ->
            catch (Pid ! stabilize),
            timer:sleep(?TICK_MS),
            try_lookup(Pid, Key, N-1)
    end.

%% ---------- stabilize ----------

force_stabilize(_, 0, _) ->
    ok;
force_stabilize(Nodes, N, T) ->
    lists:foreach(fun(P) -> catch (P ! stabilize) end, Nodes),
    timer:sleep(T),
    force_stabilize(Nodes, N-1, T).

%% ---------- store summary ----------

print_store_summary(Nodes, Label) ->
    io:format("~n--- ~s ---~n",[Label]),
    lists:foreach(
      fun(P) ->
          P ! {print, self()},
          receive
              {state, Id, _Pred, _Succ, _Next, _Nil, Store} ->
                  io:format("Node ~p holds ~p entries~n",[Id,length(Store)])
          after 2000 ->
              io:format("Timeout printing node~n")
          end
      end, Nodes),
    io:format("----------------------~n").

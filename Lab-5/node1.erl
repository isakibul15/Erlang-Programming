-module(node1).
-export([start/1, start/2, probe/1]).

-define(Stabilize, 1000).
-define(Timeout, 2000).

% API
start(Id) -> start(Id, nil).
start(Id, Peer) -> spawn(fun() -> init(Id, Peer) end).
probe(Pid) -> Pid ! probe, ok.

% Internal
init(Id, nil) ->
    %% first node: successor is self
    schedule_stabilize(),
    node(Id, nil, {Id, self()});
init(Id, Peer) ->
    {ok, Succ} = connect(Id, Peer),
    schedule_stabilize(),
    node(Id, nil, Succ).

%% Message loop. Handles ring maintenance + probe.
node(Id, Predecessor, Successor) ->
    receive
        %% Who are you?
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor);

        %% Successor tells us their current predecessor; adjust and maybe notify.
        {status, Pred} ->
            Succ2 = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, Succ2);

        %% A neighbor proposes to be our predecessor.
        {notify, New} ->
            Pred2 = notify(New, Id, Predecessor),
            node(Id, Pred2, Successor);

        %% Ask for predecessor.
        {request, Peer} ->
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor);

        %% Periodic tick
        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor);

        %% Status print
        {print, From} ->
            From ! {state, Id, Predecessor, Successor},
            node(Id, Predecessor, Successor);

        %% ---- Probe messages ----
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor);

        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor);

        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor)
    end.

%% -------- stabilization --------

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

%% Send request to successor asking for its predecessor
stabilize({_, Spid}) ->
    Spid ! {request, self()}.
    
%% Reply to a {request, Peer}. If we have no predecessor, say nil.
request(Peer, nil) ->
    Peer ! {status, nil};
request(Peer, Pred = {_K,_Pid}) ->
    Peer ! {status, Pred}.

%% Accept better predecessor if appropriate.
notify(New = {NKey, _NPid}, _Id, nil) ->
    New;
notify(New = {NKey, _NPid}, Id, Pred = {PKey, _}) ->
    case key:between(NKey, PKey, Id) of
        true  -> New;
        false -> Pred
    end.

%% Decide whether to slide successor, or just notify it.
stabilize(PredOfSucc, Id, Successor = {SKey, SPid}) ->
    case PredOfSucc of
        nil ->
            SPid ! {notify, {Id, self()}},
            Successor;
        {Id, _} ->
            Successor;
        {SKey, _} ->
            SPid ! {notify, {Id, self()}},
            Successor;
        {XKey, XPid} ->
            case key:between(XKey, Id, SKey) of
                true ->
                    XPid ! {request, self()},
                    {XKey, XPid};
                false ->
                    SPid ! {notify, {Id, self()}},
                    Successor
            end
    end.

%% -------- joining --------

connect(Id, nil) ->
    {ok, {Id, self()}};
connect(_Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive
        {Qref, SKey} -> {ok, {SKey, Peer}}
    after ?Timeout ->
        exit({timeout_connect, Peer})
    end.

%% -------- probe helpers --------

create_probe(Id, {_, Spid}) ->
    Ref = make_ref(),
    Now = erlang:system_time(microsecond),
    Spid ! {probe, Ref, [{Id, self()}], Now}.

forward_probe(Ref, T, Nodes, Id, {_, Spid}) ->
    Spid ! {probe, Ref, [{Id, self()} | Nodes], T}.

remove_probe(T, Nodes) ->
    Now = erlang:system_time(microsecond),
    DtUs = Now - T,
    Ring = lists:reverse(Nodes),
    io:format("Probe complete. Nodes=~p  time=~pus (~p ms)~n",
              [Ring, DtUs, DtUs/1000]).

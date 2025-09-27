-module(gms1_test).
-compile(export_all).

%% Test basic functionality
basic_test() ->
    %% Start first worker (leader)
    {ok, Gms1} = gms1:start(1),
    io:format("Started first node Gms1: ~p~n", [Gms1]),
    
    %% Start second worker that joins the first
    {ok, Gms2} = gms1:start(2, Gms1),
    io:format("Started second node Gms2: ~p~n", [Gms2]),
    
    %% Test multicast from leader
    Gms1 ! {mcast, hello_from_leader},
    
    %% Test multicast from slave
    Gms2 ! {mcast, hello_from_slave},
    
    timer:sleep(100),
    ok.

%% Test with the actual worker module
worker_test() ->
    %% Start first worker
    W1 = test:first(1, gms1, 1000),
    io:format("First worker started: ~p~n", [W1]),
    
    timer:sleep(2000),
    
    %% Start second worker that joins the first
    W2 = test:add(2, gms1, W1, 1000),
    io:format("Second worker started: ~p~n", [W2]),
    
    timer:sleep(5000),
    
    %% Test multicast
    test:freeze(W1),
    timer:sleep(2000),
    test:go(W1),
    
    ok.
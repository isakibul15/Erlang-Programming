-module(gms3_test).
-compile(export_all).

reliable_test() ->
    io:format("=== Testing gms3 Reliable Multicast ===~n"),
    
    %% Start three workers
    W1 = worker:start(1, gms3, 111, 1000),
    timer:sleep(2000),
    W2 = worker:start(2, gms3, 222, W1, 1000),
    timer:sleep(2000),
    W3 = worker:start(3, gms3, 333, W1, 1000),
    timer:sleep(5000),
    W4 = worker:start(4, gms3, 444, W1, 1000),
    timer:sleep(5000),
    
    %% Test reliable multicast
    io:format("Testing reliable multicast...~n"),
    timer:sleep(5000),
    
    %% Cleanup
    W1 ! stop,
    W2 ! stop,
    W3 ! stop,
    W4 ! stop,
    ok.
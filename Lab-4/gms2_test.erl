-module(gms2_test).
-compile(export_all).

%% Test failure detection and leader election
failure_test() ->
    io:format("=== Testing Failure Detection ===~n"),
    
    %% Start three workers
    W1 = worker:start(1, gms2, 111, 1000),
    timer:sleep(2000),
    W2 = worker:start(2, gms2, 222, W1, 1000),
    timer:sleep(2000),
    W3 = worker:start(3, gms2, 333, W1, 1000),
    timer:sleep(5000),
    
    %% Kill the leader (W1)
    io:format("Killing leader (Worker 1)...~n"),
    exit(W1, kill),
    timer:sleep(3000),
    
    %% Verify new leader took over and synchronization continues
    io:format("Testing synchronization after leader failure...~n"),
    timer:sleep(5000),
    
    %% Cleanup
    W2 ! stop,
    W3 ! stop,
    ok.
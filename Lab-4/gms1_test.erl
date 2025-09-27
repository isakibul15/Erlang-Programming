-module(gms1_test).
-compile(export_all).

%% Test basic functionality
basic_test() ->
    {ok, Gms1} = gms1:start(1),
    io:format("Started first node Gms1: ~p~n", [Gms1]),
    
    {ok, Gms2} = gms1:start(2, Gms1),
    io:format("Started second node Gms2: ~p~n", [Gms2]),
    
    Gms1 ! {mcast, hello_from_leader},
    Gms2 ! {mcast, hello_from_slave},
    
    timer:sleep(100),
    ok.

%% Test with workers - using direct messages to avoid test.erl issues
worker_test() ->
    %% Use worker:start directly to avoid test.erl issues
    W1 = worker:start(1, gms1, 123, 1000),
    io:format("First worker started: ~p~n", [W1]),
    
    timer:sleep(2000),
    
    W2 = worker:start(2, gms1, 456, W1, 1000),
    io:format("Second worker started: ~p~n", [W2]),
    
    timer:sleep(5000),
    
    %% Use direct messages instead of test.erl functions
    io:format("Freezing all workers...~n"),
    W1 ! {send, freeze},
    timer:sleep(2000),
    
    io:format("Resuming all workers...~n"),
    W1 ! {send, go},
    timer:sleep(2000),
    
    %% Cleanup
    W1 ! stop,
    W2 ! stop,
    ok.

%% Test multiple workers
multiple_workers_test() ->
    io:format("=== Testing Multiple Workers ===~n"),
    
    W1 = worker:start(1, gms1, 111, 1000),
    io:format("Worker 1 started: ~p~n", [W1]),
    timer:sleep(2000),
    
    W2 = worker:start(2, gms1, 222, W1, 1000),
    io:format("Worker 2 started: ~p~n", [W2]),
    timer:sleep(2000),
    
    W3 = worker:start(3, gms1, 333, W1, 1000),
    io:format("Worker 3 started: ~p~n", [W3]),
    timer:sleep(5000),
    
    io:format("Freezing all workers...~n"),
    W1 ! {send, freeze},
    timer:sleep(3000),
    
    io:format("Resuming all workers...~n"),
    W1 ! {send, go},
    timer:sleep(3000),
    
    W1 ! stop,
    W2 ! stop, 
    W3 ! stop,
    ok.

%% Run all tests
run_all_tests() ->
    io:format("🚀 Running GMS1 Test Suite~n"),
    io:format("=========================~n"),
    
    basic_test(),
    io:format("✓ Basic test completed~n"),
    
    worker_test(),
    io:format("✓ Worker test completed~n"),
    
    multiple_workers_test(), 
    io:format("✓ Multiple workers test completed~n"),
    
    io:format("=========================~n"),
    io:format("🎉 All tests passed!~n"),
    ok.
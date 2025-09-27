-module(test_suite).
-compile(export_all).

%% Comprehensive test suite for all gms versions

run_all_tests() ->
    io:format("=== Running Complete Test Suite ===~n"),
    
    io:format("~n1. Testing gms1 (Basic Group Membership)~n"),
    test_gms1(),
    
    io:format("~n2. Testing gms2 (Failure Detection)~n"),
    test_gms2(),
    
    io:format("~n3. Testing gms3 (Reliable Multicast)~n"),
    test_gms3(),
    
    io:format("~n=== All Tests Completed ===~n"),
    ok.

test_gms1() ->
    io:format("Starting gms1 test...~n"),
    
    %% Create group
    W1 = test:first(1, gms1, 1000),
    timer:sleep(2000),
    W2 = test:add(2, gms1, W1, 1000),
    timer:sleep(2000),
    W3 = test:add(3, gms1, W1, 1000),
    timer:sleep(5000),
    
    %% Test basic functionality
    io:format("Testing basic commands...~n"),
    test:freeze(W1),
    timer:sleep(2000),
    test:go(W1),
    timer:sleep(2000),
    
    %% Test speed change
    test:sleep(W1, 500),
    timer:sleep(3000),
    
    %% Cleanup
    test:stop(W1),
    io:format("gms1 test completed successfully.~n"),
    ok.

test_gms2() ->
    io:format("Starting gms2 test...~n"),
    
    %% Create group
    W1 = test:first(1, gms2, 1000),
    timer:sleep(2000),
    W2 = test:add(2, gms2, W1, 1000),
    timer:sleep(2000),
    W3 = test:add(3, gms2, W1, 1000),
    timer:sleep(5000),
    
    %% Test before failure
    io:format("Testing before leader failure...~n"),
    test:freeze(W1),
    timer:sleep(2000),
    test:go(W1),
    timer:sleep(2000),
    
    %% Test leader failure
    io:format("Simulating leader failure...~n"),
    exit(W1, kill),
    timer:sleep(3000),
    
    %% Test after failure
    io:format("Testing after leader failure...~n"),
    test:freeze(W2),
    timer:sleep(2000),
    test:go(W2),
    timer:sleep(2000),
    
    %% Cleanup
    test:stop(W2),
    io:format("gms2 test completed successfully.~n"),
    ok.

test_gms3() ->
    io:format("Starting gms3 test...~n"),
    
    %% Create group
    W1 = test:first(1, gms3, 1000),
    timer:sleep(2000),
    W2 = test:add(2, gms3, W1, 1000),
    timer:sleep(2000),
    W3 = test:add(3, gms3, W1, 1000),
    timer:sleep(5000),
    
    %% Test reliable multicast
    io:format("Testing reliable multicast...~n"),
    test:freeze(W1),
    timer:sleep(2000),
    test:go(W1),
    timer:sleep(2000),
    
    %% Test with leader failure
    io:format("Testing with leader failure...~n"),
    exit(W1, kill),
    timer:sleep(3000),
    
    %% Test recovery
    io:format("Testing recovery...~n"),
    test:freeze(W2),
    timer:sleep(2000),
    test:go(W2),
    timer:sleep(2000),
    
    %% Cleanup
    test:stop(W2),
    io:format("gms3 test completed successfully.~n"),
    ok.

%% Individual test functions for debugging
quick_test_gms1() ->
    W1 = test:first(1, gms1, 1000),
    timer:sleep(3000),
    test:stop(W1).

quick_test_gms2() ->
    W1 = test:first(1, gms2, 1000),
    timer:sleep(2000),
    W2 = test:add(2, gms2, W1, 1000),
    timer:sleep(3000),
    exit(W1, kill),
    timer:sleep(2000),
    test:stop(W2).

quick_test_gms3() ->
    W1 = test:first(1, gms3, 1000),
    timer:sleep(2000),
    W2 = test:add(2, gms3, W1, 1000),
    timer:sleep(3000),
    test:stop(W1).
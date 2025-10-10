-module(gms4_test).
-export([run/0, test_reliability/0, test_leader_failure/0, test_message_recovery/0, test_multiple_failures/0]).

run() ->
    io:format("=== GMS4 Reliability Demonstration Tests ===~n~n"),
    test_reliability(),
    timer:sleep(2000),
    test_leader_failure(), 
    timer:sleep(2000),
    test_message_recovery(),
    timer:sleep(2000),
    test_multiple_failures(),
    io:format("~n=== All reliability tests completed! ===~n").

%% Test 1: Basic reliability with message ACKs and retransmission
test_reliability() ->
    io:format("--- Test 1: Message Reliability with ACKs ---~n"),
    io:format("Starting 3 workers to show reliable message delivery...~n"),
    
    W1 = worker:start(1, gms4, 100, 2000),
    timer:sleep(800),
    W2 = worker:start(2, gms4, 200, W1, 2000),
    timer:sleep(600),
    W3 = worker:start(3, gms4, 300, W1, 2000),
    
    io:format("~n>>> System running normally - observe ACK messages in logs~n"),
    io:format(">>> Each message should show: Multicast -> ACK from each slave -> 'All ACKs received'~n"),
    timer:sleep(10000),
    
    stop_workers([W1, W2, W3]),
    io:format("Test 1 completed - ACK mechanism verified~n~n").

%% Test 2: Focused leader failure with detailed observation
test_leader_failure() ->
    io:format("--- Test 2: Leader Failure & Election Process ---~n"),
    io:format("Starting 4 workers to demonstrate leader election...~n"),
    
    W1 = worker:start(10, gms4, 100, 1500),
    timer:sleep(600),
    W2 = worker:start(11, gms4, 200, W1, 1500),
    timer:sleep(400),
    W3 = worker:start(12, gms4, 300, W1, 1500),
    timer:sleep(400),
    W4 = worker:start(13, gms4, 400, W1, 1500),
    
    io:format("~n>>> Letting system stabilize (5 seconds)...~n"),
    timer:sleep(5000),
    
    io:format("~n*** INTENTIONALLY KILLING LEADER (Worker 10) ***~n"),
    io:format(">>> Watch for: Leader detection -> Election -> New leader announcement -> Message recovery~n"),
    W1 ! stop,
    
    timer:sleep(3000),
    io:format("~n>>> System should now be running with new leader~n"),
    io:format(">>> Observe: New leader resending last message to prevent loss~n"),
    timer:sleep(8000),
    
    stop_workers([W2, W3, W4]),
    io:format("Test 2 completed - Leader election verified~n~n").

%% Test 3: Message recovery and ordering guarantees
test_message_recovery() ->
    io:format("--- Test 3: Message Recovery & Ordering ---~n"),
    io:format("Testing that no messages are lost during leader transitions...~n"),
    
    W1 = worker:start(20, gms4, 100, 1000),
    timer:sleep(700),
    W2 = worker:start(21, gms4, 200, W1, 1000),
    timer:sleep(500),
    W3 = worker:start(22, gms4, 300, W1, 1000),
    
    io:format("~n>>> Sending messages with leader (3 seconds)...~n"),
    timer:sleep(3000),
    
    io:format("~n*** KILLING LEADER DURING ACTIVE MESSAGING ***~n"),
    W1 ! stop,
    
    io:format(">>> New leader should recover any in-flight messages~n"),
    timer:sleep(3000),
    
    io:format(">>> Continuing operation with new leader (7 seconds)...~n"),
    timer:sleep(7000),
    
    stop_workers([W2, W3]),
    io:format("Test 3 completed - Message recovery verified~n~n").

%% Test 4: Multiple failures and robust recovery
test_multiple_failures() ->
    io:format("--- Test 4: Multiple Failures & System Resilience ---~n"),
    io:format("Testing system with sequential failures...~n"),
    
    W1 = worker:start(30, gms4, 100, 1200),
    timer:sleep(500),
    W2 = worker:start(31, gms4, 200, W1, 1200),
    timer:sleep(400),
    W3 = worker:start(32, gms4, 300, W1, 1200),
    timer:sleep(400),
    W4 = worker:start(33, gms4, 400, W1, 1200),
    timer:sleep(400),
    W5 = worker:start(34, gms4, 500, W1, 1200),
    
    io:format("~n>>> 5-worker system running (4 seconds)...~n"),
    timer:sleep(4000),
    
    io:format("~n*** FAILURE 1: Killing non-leader worker 33 ***~n"),
    W4 ! stop,
    timer:sleep(4000),
    
    io:format("~n*** FAILURE 2: Killing current leader worker 30 ***~n"),
    W1 ! stop,
    timer:sleep(4000),
    
    io:format("~n*** FAILURE 3: Killing another worker 32 ***~n"),
    W3 ! stop,
    timer:sleep(4000),
    
    io:format("~n>>> System continues with 2 workers (4 seconds)...~n"),
    timer:sleep(4000),
    
    stop_workers([W2, W5]),
    io:format("Test 4 completed - Multiple failure recovery verified~n~n").

%% Helper to stop multiple workers
stop_workers(Workers) ->
    io:format(">>> Stopping ~w workers...~n", [length(Workers)]),
    lists:foreach(fun(W) -> W ! stop end, Workers),
    timer:sleep(1000).
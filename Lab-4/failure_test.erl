-module(failure_test).
-compile(export_all).

%% Specialized tests for failure scenarios

test_message_loss_prevention() ->
    io:format("=== Testing Message Loss Prevention ===~n"),
    
    %% Create gms3 group (most reliable)
    W1 = test:first(1, gms3, 500),
    timer:sleep(2000),
    W2 = test:add(2, gms3, W1, 500),
    timer:sleep(2000),
    W3 = test:add(3, gms3, W1, 500),
    timer:sleep(5000),
    
    %% Send multiple messages quickly
    io:format("Sending rapid messages...~n"),
    test:freeze(W1),
    test:sleep(W1, 100),
    test:go(W1),
    timer:sleep(1000),
    
    %% Kill leader during message flow
    io:format("Killing leader during activity...~n"),
    exit(W1, kill),
    timer:sleep(3000),
    
    %% Verify synchronization maintained
    io:format("Verifying synchronization...~n"),
    test:freeze(W2),
    timer:sleep(2000),
    test:go(W2),
    timer:sleep(2000),
    
    test:stop(W2),
    io:format("Message loss prevention test completed.~n").

test_multiple_failures() ->
    io:format("=== Testing Multiple Sequential Failures ===~n"),
    
    %% Create larger group
    Workers = [test:first(1, gms2, 1000)],
    timer:sleep(2000),
    
    %% Add more workers
    lists:foreach(fun(N) ->
        timer:sleep(1000),
        W = test:add(N, gms2, hd(Workers), 1000),
        Workers = [W | Workers]
    end, lists:seq(2, 4)),
    
    timer:sleep(5000),
    
    %% Sequential failures
    io:format("Testing sequential leader failures...~n"),
    [First | Rest] = Workers,
    exit(First, kill),  % Kill first leader
    timer:sleep(3000),
    
    NewLeader = hd(Rest),
    exit(NewLeader, kill),  % Kill second leader
    timer:sleep(3000),
    
    %% System should still work
    test:freeze(hd(tl(Rest))),
    timer:sleep(2000),
    test:go(hd(tl(Rest))),
    timer:sleep(2000),
    
    %% Cleanup remaining workers
    lists:foreach(fun(W) -> test:stop(W) end, tl(tl(Rest))),
    io:format("Multiple failures test completed.~n").

test_join_during_failure() ->
    io:format("=== Testing Join During Failure ===~n"),
    
    W1 = test:first(1, gms2, 1000),
    timer:sleep(2000),
    W2 = test:add(2, gms2, W1, 1000),
    timer:sleep(2000),
    
    %% Kill leader and immediately try to join
    exit(W1, kill),
    
    %% Try to join during election
    timer:sleep(1000),
    case (catch test:add(3, gms2, W2, 1000)) of
        {'EXIT', _} ->
            io:format("Join failed during election (expected)~n");
        W3 ->
            io:format("Join succeeded during election~n"),
            timer:sleep(3000),
            test:stop(W3)
    end,
    
    timer:sleep(3000),
    test:stop(W2),
    io:format("Join during failure test completed.~n").
-module(test).
-compile(export_all).

-define(Timeout, 1000).

%% Starting up a set of nodes is made easier using this function.
start(Module) ->
    Id = key:generate(), 
    apply(Module, start, [Id]).

start(Module, P) ->
    Id = key:generate(), 
    apply(Module, start, [Id,P]).    

start(_, 0, _) ->
    ok;
start(Module, N, P) ->
    start(Module, P),
    start(Module, N-1, P).

%% The functions add and lookup can be used to test if a DHT works.
add(Key, Value , P) ->
    Q = make_ref(),
    P ! {add, Key, Value, Q, self()},
    receive 
        {Q, ok} ->
            ok
    after ?Timeout ->
        {error, "timeout"}
    end.

lookup(Key, Node) ->
    Q = make_ref(),
    Node ! {lookup, Key, Q, self()},
    receive 
        {Q, Value} ->
            Value
    after ?Timeout ->
        {error, "timeout"}
    end.

%% Replication test: verify that data survives node failures
test_replication() ->
    io:format("Testing replication...~n"),
    
    % Start three nodes
    N1 = node4:start(nil),
    timer:sleep(1000),
    N2 = node4:start(N1),
    timer:sleep(1000),
    N3 = node4:start(N1),
    timer:sleep(2000),
    
    % Add some test data
    TestKey = 123456,
    TestValue = test_data,
    add(TestKey, TestValue, N1),
    timer:sleep(500),
    
    % Verify data is accessible
    case lookup(TestKey, N1) of
        {TestKey, TestValue} ->
            io:format("Data found in ring~n");
        Other ->
            io:format("Unexpected lookup result: ~p~n", [Other])
    end,
    
    % Simulate node failure (kill N2 which should have replica of N1's data)
    io:format("Simulating node failure...~n"),
    exit(N2, kill),
    timer:sleep(2000),
    
    % Try to lookup data again - it should survive due to replication
    case lookup(TestKey, N1) of
        {TestKey, TestValue} ->
            io:format("SUCCESS: Data survived node failure due to replication!~n");
        {error, _} ->
            io:format("Lookup timeout after node failure~n");
        false ->
            io:format("FAIL: Data lost after node failure~n");
        Other2 ->
            io:format("Unexpected result after failure: ~p~n", [Other2])
    end,
    
    % Cleanup
    N1 ! stop,
    N3 ! stop.

%% Benchmark with replication
keys(N) ->
    lists:map(fun(_) -> key:generate() end, lists:seq(1,N)).

add_keys(Keys, P) ->
    lists:foreach(fun(K) -> add(K, gurka, P) end, Keys).

check(Keys, P) ->
    T1 = now(),
    {Failed, Timeout} = check(Keys, P, 0, 0),
    T2 = now(),
    Done = (timer:now_diff(T2, T1) div 1000),
    io:format("~w lookup operations in ~w ms ~n", [length(Keys), Done]),
    io:format("~w lookups failed, ~w caused a timeout ~n", [Failed, Timeout]),
    {Failed, Timeout}.

check([], _, Failed, Timeout) ->
    {Failed, Timeout};
check([Key|Keys], P, Failed, Timeout) ->
    case lookup(Key,P) of
        {Key, _} -> 
            check(Keys, P, Failed, Timeout);
        {error, _} -> 
            check(Keys, P, Failed, Timeout+1);
        false ->
            check(Keys, P, Failed+1, Timeout)
    end.

%% Performance test with replication
performance_test() ->
    io:format("Starting performance test with replication...~n"),
    
    % Create a ring with 4 nodes
    Nodes = [node4:start(nil)],
    timer:sleep(1000),
    
    Nodes2 = [node4:start(hd(Nodes)) | Nodes],
    timer:sleep(1000),
    
    Nodes3 = [node4:start(hd(Nodes2)) | Nodes2],
    timer:sleep(1000),
    
    Nodes4 = [node4:start(hd(Nodes3)) | Nodes3],
    timer:sleep(2000),
    
    % Generate test keys
    TestKeys = keys(100),
    
    % Add keys to the ring
    io:format("Adding ~w keys to the ring...~n", [length(TestKeys)]),
    add_keys(TestKeys, hd(Nodes4)),
    timer:sleep(1000),
    
    % Test lookups
    io:format("Testing lookups...~n"),
    check(TestKeys, hd(Nodes4)),
    
    % Cleanup
    lists:foreach(fun(Node) -> Node ! stop end, Nodes4).

run_all_tests() ->
    io:format("=== Running Replication Tests ===~n"),
    test_replication(),
    io:format("~n=== Running Performance Tests ===~n"),
    performance_test().
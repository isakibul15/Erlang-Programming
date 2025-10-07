-module(test).
-compile(export_all).

-define(Timeout, 2000).

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
    
    % Start three nodes with proper delays for stabilization
    N1 = node4:start(nil),
    timer:sleep(2000),
    N2 = node4:start(N1),
    timer:sleep(2000),
    N3 = node4:start(N1),
    timer:sleep(2000),
    
    % Add some test data
    TestKey = 123456,
    TestValue = test_data,
    io:format("Adding test data...~n"),
    case add(TestKey, TestValue, N1) of
        ok -> io:format("Data added successfully~n");
        Error -> io:format("Failed to add data: ~p~n", [Error])
    end,
    timer:sleep(1000),
    
    % Verify data is accessible from all nodes
    io:format("Verifying data from all nodes...~n"),
    check_from_all([N1, N2, N3], TestKey, TestValue),
    
    % Simulate node failure (kill N2 which should have replica of N1's data)
    io:format("Simulating node failure...~n"),
    exit(N2, kill),
    timer:sleep(3000),
    
    % Try to lookup data again - it should survive due to replication
    io:format("Checking data survival after node failure...~n"),
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

check_from_all([], _Key, _Value) -> ok;
check_from_all([Node|Nodes], Key, ExpectedValue) ->
    case lookup(Key, Node) of
        {Key, ExpectedValue} ->
            io:format("Node ~p: Data found~n", [Node]),
            check_from_all(Nodes, Key, ExpectedValue);
        Other ->
            io:format("Node ~p: Unexpected result ~p~n", [Node, Other]),
            check_from_all(Nodes, Key, ExpectedValue)
    end.

%% Benchmark with replication
keys(N) ->
    lists:map(fun(_) -> key:generate() end, lists:seq(1,N)).

add_keys(Keys, P) ->
    lists:foreach(fun(K) -> 
        case add(K, gurka, P) of
            ok -> ok;
            Error -> io:format("Failed to add key ~p: ~p~n", [K, Error])
        end
    end, Keys).

check(Keys, P) ->
    T1 = erlang:monotonic_time(millisecond),
    {Failed, Timeout} = check(Keys, P, 0, 0),
    T2 = erlang:monotonic_time(millisecond),
    Done = T2 - T1,
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
    
    % Create a ring with 2 nodes first
    N1 = node4:start(nil),
    timer:sleep(2000),
    
    N2 = node4:start(N1),
    timer:sleep(2000),
    
    % Generate test keys
    TestKeys = keys(50),  % Reduced for testing
    
    % Add keys to the ring
    io:format("Adding ~w keys to the ring...~n", [length(TestKeys)]),
    add_keys(TestKeys, N1),
    timer:sleep(2000),
    
    % Test lookups
    io:format("Testing lookups...~n"),
    check(TestKeys, N1),
    
    % Cleanup
    N1 ! stop,
    N2 ! stop.

run_all_tests() ->
    io:format("=== Running Replication Tests ===~n"),
    test_replication(),
    io:format("~n=== Running Performance Tests ===~n"),
    performance_test().

%% Simple test for basic functionality
simple_test() ->
    io:format("Simple test: starting single node~n"),
    N1 = node4:start(nil),
    timer:sleep(1000),
    
    % Add and lookup a single key
    Key = 123,
    Value = test_value,
    add(Key, Value, N1),
    timer:sleep(500),
    
    case lookup(Key, N1) of
        {Key, Value} -> io:format("SUCCESS: Basic add/lookup works~n");
        Other -> io:format("FAIL: Basic test failed: ~p~n", [Other])
    end,
    
    N1 ! stop.
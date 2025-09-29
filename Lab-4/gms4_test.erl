-module(gms4_test).
-compile(export_all).

%% Lossy replay and failover demo for gms4

lossy_replay_demo() ->
    io:format("=== gms4: lossy replay + failover demo ===~n"),
    rand:seed(exsplus, {101,102,103}),

    %% Start three workers on gms4
    W1 = worker:start(1, gms4, 111, 400),
    timer:sleep(1000),
    W2 = worker:start(2, gms4, 222, W1, 400),
    timer:sleep(800),
    W3 = worker:start(3, gms4, 333, W1, 400),
    timer:sleep(1200),

    %% Turn on lossy multicast at the leader via control multicast
    io:format("Enable lossy 30% at leader~n"),
    W1 ! {send, {ctl_set_lossy, true, 30}},
    timer:sleep(500),

    %% Generate traffic from all workers
    io:format("Traffic under loss...~n"),
    W2 ! {send, {change, 7}},
    W3 ! {send, {change, 9}},
    timer:sleep(2000),

    %% Kill leader to force election; re-enable lossy at new leader
    io:format("Kill leader worker 1~n"),
    exit(W1, kill),
    timer:sleep(1500),

    io:format("Re-enable lossy 30% at current leader via W2~n"),
    W2 ! {send, {ctl_set_lossy, true, 30}},
    timer:sleep(500),

    %% More traffic to force NACK/replay under new leader
    io:format("More traffic under loss after failover...~n"),
    W2 ! {send, {change, 13}},
    W3 ! {send, {change, 5}},
    timer:sleep(2000),

    %% Cleanup
    io:format("Cleanup~n"),
    W2 ! stop,
    W3 ! stop,
    ok.

quick() ->
    %% simple smoke
    W1 = worker:start(1, gms4, 111, 500),
    timer:sleep(800),
    W2 = worker:start(2, gms4, 222, W1, 500),
    timer:sleep(800),
    W1 ! {send, {ctl_set_lossy, true, 20}},
    timer:sleep(1500),
    exit(W1, kill),
    timer:sleep(1200),
    W2 ! stop,
    ok.

-module(hello).
-export([start/0, stop/0, hello/0]).
start() ->
    register(hello, spawn(fun hello/0)).
stop() ->
    unregister(hello).
hello() ->
    io:format("Hello, world!~n").
-module(sakib).
-export([hello/0]).

hello() ->
    receive
        Msg ->
            io:format("Hi! I am sakib, I got a message: ~p~n", [Msg]),
            hello()  
    end.

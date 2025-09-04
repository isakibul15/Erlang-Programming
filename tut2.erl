-module(tut2).
-export([sum/1, member/2]).

sum(L) -> 
    case L of 
        [] -> 
            0;
        [H|T] -> 
            H + sum(T)
    end.


member(X,L) ->
    case L of 
        [] -> 
            no;
        [X|_] -> 
            yes;
        [_|T] -> 
            member(X,T)
    end.
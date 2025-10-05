%% test_assert.erl
-module(test_assert).
-compile(export_all).

%% Query K workers for current color and assert they match
assert_converged(Workers, K, Timeout) ->
    Ws = lists:sublist(Workers, min(K, length(Workers))),
    Ref = make_ref(),
    lists:foreach(fun(W) -> W ! {send, {state_request, Ref}} end, Ws),
    Colors = collect(Ref, Timeout, []),
    case lists:usort(Colors) of
        []  -> ok;                              %% nothing returned yet
        [_] -> ok;                              %% converged
        Set -> erlang:error({diverged, Set})    %% mismatch
    end.

collect(Ref, Timeout, Acc) ->
    receive
        {state, Ref, Color} -> collect(Ref, Timeout, [Color|Acc])
    after Timeout -> Acc
    end.

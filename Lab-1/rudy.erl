-module(rudy).
-export([start/1, stop/0]).

start(Port) ->
    register(rudy, spawn(fun() -> init(Port) end)).

stop() ->
    exit(whereis(rudy), "time to die").

init(Port) ->
    Opt = [list, {active, false}, {reuseaddr, true}],
    case gen_tcp:listen(Port, Opt) of
        {ok, Listen} ->
            % This is where we handle incoming connections
            handler(Listen),
            % After handling, close the listening socket
            gen_tcp:close(Listen),
            ok;
        {error, Error} ->
            error
    end.

handler(Listen) ->
    case gen_tcp:accept(Listen) of
        {ok, Client} ->
            % Handle the client request
            request(Client),
            % Close the client connection
            gen_tcp:close(Client),
            % For now, terminate after one request
            handler(Listen);
        {error, Error} ->
            error
    end.

request(Client) ->
    Recv = gen_tcp:recv(Client, 0),
    case Recv of
        {ok, Str} ->
            % Parse the HTTP request
            Request = http:parse_request(Str),
            % Generate a response
            Response = reply(Request),
            % Send the response back to client
            gen_tcp:send(Client, Response);
        {error, Error} ->
            io:format("rudy: error: ~w~n", [Error])
    end.

reply({{get, _URI, _}, _Headers, _Body}) ->
    http:ok("Hello from Rudy Server!");
reply(_) ->
    http:ok("Unsupported request type").

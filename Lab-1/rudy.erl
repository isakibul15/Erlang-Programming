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
            handler(Listen),
            gen_tcp:close(Listen),
            ok;
        {error, Error} ->
            error
    end.

handler(Listen) ->
    case gen_tcp:accept(Listen) of
        {ok, Client} ->
            request(Client),
            gen_tcp:close(Client),
            handler(Listen);
        {error, Error} ->
            error
    end.

request(Client) ->
    Recv = gen_tcp:recv(Client, 0),
    case Recv of
        {ok, Str} ->
            Request = http:parse_request(Str),
            Response = reply(Request),
            gen_tcp:send(Client, Response);
        {error, Error} ->
            io:format("rudy: error: ~w~n", [Error])
    end.

% reply({{get, _URI, _}, _Headers, _Body}) ->
%     timer:sleep(40),
%     http:ok("Hello from Rudy Server!");
% reply(_) ->
%     timer:sleep(40),
%     http:ok("Unsupported request type").

% Handle GET request and serve files
reply({{get, URI, _}, _Headers, _Body}) ->
    io:format("Requested URI: ~p~n", [URI]),
    FilePath = extract_path(URI),
    io:format("Looking for file: ~p~n", [FilePath]),
    
    case filelib:is_regular(FilePath) of
        true ->
            io:format("File found!~n"),
            http:serve_file(FilePath);
        false ->
            io:format("File NOT found!~n"),
            http:not_found()
    end;
reply(_) ->
    http:not_found().

% Helper to extract file path from URI
% Replace the entire extract_path/1 function with this improved version
extract_path(URI) ->
    Parts = string:split(URI, "?", leading),
    
    Path = case Parts of
        [P | _] -> P;
        [] -> "/"
    end,
    
    case Path of
        "/" -> "index.html";
        "" -> "index.html";
        _ -> 
            string:substr(Path, 2)
    end.
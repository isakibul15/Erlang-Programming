-module(http).
-export([parse_request/1, ok/1, get/1, not_found/0, not_found/1, serve_file/1]).

% HTTP request structure 
% R0 means it takes raw request as input
% Request-Line = Method SP Request-URI SP HTTP-Version CRLF
% So "GET /index.html HTTP/1.1\r\n"
parse_request(R0) ->
    {Request, R1} = request_line(R0),
    {Headers, R2} = headers(R1),
    {Body, _} = message_body(R2),
    {Request, Headers, Body}.

% Parse Request-Line
request_line([$G, $E, $T, 32 |R0]) ->
    {URI, R1} = request_uri(R0),
    {Ver, R2} = http_version(R1),
    [13, 10|R3] = R2,
    {{get, URI, Ver}, R3}.

% Parse Request-URI until space (32)
request_uri([32|R0]) ->
    {[], R0};
request_uri([C|R0]) ->
    {Rest, R1} = request_uri(R0),
    {[C|Rest], R1}.

% Parse HTTP-Version Information
% Atoms are easier to match than strings
http_version([$H, $T, $T, $P, $/, $1, $., $1 |R0]) -> 
    {v11, R0};
http_version([$H, $T, $T, $P, $/, $1, $., $0 |R0]) ->
    {v10, R0}.


headers([13, 10|R0]) -> 
    {[], R0};
headers(R0) ->
    {Header, R1} = header(R0),
    {Rest, R2} = headers(R1),
    {[Header | Rest], R2}.

header([13,10|R0]) -> 
    {[], R0};
header([C|R0]) ->
    {Rest, R1} = header(R0),
    {[C|Rest], R1}.

message_body(R) ->
    {R, []}.

ok(Body) ->
    "HTTP/1.1 200 OK\r\n" ++ "\r\n" ++ Body.

get(URI) -> 
    "GET " ++ URI ++ " HTTP/1.1\r\n" ++ "\r\n".

% Generate 404 response
not_found() ->
    "HTTP/1.1 404 Not Found\r\n\r\nFile Not Found".
not_found(Body) ->
    "HTTP/1.1 404 Not Found\r\n\r\n" ++ Body.

% Serve file with proper headers
serve_file(Path) ->
    case file:read_file(Path) of
        {ok, Content} ->
            Size = integer_to_list(byte_size(Content)),
            Type = content_type(Path),
            "HTTP/1.1 200 OK\r\n" ++
            "Content-Type: " ++ Type ++ "\r\n" ++
            "Content-Length: " ++ Size ++ "\r\n" ++
            "\r\n" ++
            binary_to_list(Content);
        {error, _} ->
            not_found()
    end.


content_type(Path) ->
    case filename:extension(Path) of
        ".html" -> "text/html";
        ".htm" -> "text/html";
        ".css" -> "text/css";
        ".js" -> "application/javascript";
        ".jpg" -> "image/jpeg";
        ".jpeg" -> "image/jpeg";
        ".png" -> "image/png";
        ".gif" -> "image/gif";
        ".txt" -> "text/plain";
        _ -> "application/octet-stream"
    end.
-module(riakc_ts).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-export([query/2,
         query/3,
         put/3,
         put/4,
         cell_for/1]).

-ifndef(SINT64_MIN).
-define(SINT64_MIN, -16#8000000000000000).
-endif.
-ifndef(SINT64_MAX).
-define(SINT64_MAX,  16#7FFFFFFFFFFFFFFF).
-endif.

query(Pid, QueryText) ->
    query(Pid, QueryText, []).

query(Pid, QueryText, Interpolations) ->
    Message = riakc_ts_query_operator:serialize(QueryText, Interpolations),
    Response = server_call(Pid, Message),
    riakc_ts_query_operator:deserialize(Response).

put(Pid, TableName, Measurements) ->
    put(Pid, TableName, undefined, Measurements).

put(Pid, TableName, Columns, Measurements) ->
    Message = riakc_ts_put_operator:serialize(TableName, Columns, Measurements),
    Response = server_call(Pid, Message),
    riakc_ts_put_operator:deserialize(Response).

server_call(Pid, Message) ->
    gen_server:call(Pid, 
                    {req, Message, riakc_pb_socket:default_timeout(timeseries)},
                    infinity).

cell_for(Measure) when is_binary(Measure) ->
    #tscell{binary_value = Measure};
cell_for(Measure) when is_integer(Measure),
                       (?SINT64_MIN =< Measure),
                       (Measure =< ?SINT64_MAX)  ->
    #tscell{integer_value = Measure};
cell_for(Measure) when is_integer(Measure) ->
    #tscell{numeric_value = integer_to_list(Measure)};
cell_for(Measure) when is_float(Measure) ->
    #tscell{numeric_value = float_to_list(Measure)};
cell_for({time, Measure}) ->
    #tscell{timestamp_value = Measure};
cell_for(true) ->
    #tscell{boolean_value = true};
cell_for(false) ->
    #tscell{boolean_value = false}.

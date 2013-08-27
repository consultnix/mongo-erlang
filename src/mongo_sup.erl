-module(mongo_sup).
-export([
	start_link/0
]).

-export([
	start_client/4,
	start_connection/4,
	start_cursor/7
]).

-behaviour(supervisor).
-export ([
	init/1
]).

-define(SUPERVISOR(Id),         {Id, {supervisor, start_link, [?MODULE, Id]}, permanent, infinity, supervisor, [?MODULE]}).
-define(SUPERVISOR(Id, Name),   {Id, {supervisor, start_link, [{local, Name}, ?MODULE, Id]}, permanent, infinity, supervisor, [?MODULE]}).
-define(WORKER(M, F, A, R),     {M,  {M, F, A}, R, 5000, worker, [M]}).
-define(WORKER(Id, M, F, A, R), {Id,  {M, F, A}, R, 5000, worker, [M]}).


-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, app).

-spec start_client(mongo_client:id(), mongo_client:host(), 1..65535, mongo_client:options()) -> {ok, pid()}.
start_client(Id, Host, Port, Options) ->
	Args = [Id, Host, Port, Options],
	supervisor:start_child(mongo_clients_sup, ?WORKER(Id, mongo_client, start_link, Args, transient)).

-spec start_connection(mongo_client:id(), mongo_client:host(), 1..65535, mongo_connection:options()) -> {ok, pid()}.
start_connection(Client, Host, Port, Options) ->
	supervisor:start_child(mongo_connections_sup, [Client, Host, Port, Options]).

-spec start_cursor(pid(), pid(), mongo:database(), mongo:collection(), integer(), integer(), [bson:document()]) -> {ok, pid()}.
start_cursor(Owner, Connection, Database, Collection, CursorId, BatchSize, Batch) ->
	supervisor:start_child(mongo_cursors_sup, [Owner, Connection, Database, Collection, CursorId, BatchSize, Batch]).

%% @hidden
init(app) ->
	mongo = ets:new(mongo, [public, named_table, ordered_set, {read_concurrency, true}]),
	ets:insert(mongo, [
		{oid_local_id, oid_local_id()},
		{oid_counter, 0}
	]),
	{ok, {
		{one_for_one, 10, 10}, [
			?SUPERVISOR(clients, mongo_clients_sup),
			?SUPERVISOR(connections, mongo_connections_sup),
			?SUPERVISOR(cursors, mongo_cursors_sup)
		]
	}};
init(clients) ->
	{ok, {
		{one_for_one, 10, 10}, []
	}};
init(connections) ->
	{ok, {
		{simple_one_for_one, 1000, 10}, [
			?WORKER(mongo_connection, start_link, [], transient)
		]
	}};
init(cursors) ->
	{ok, {
		{simple_one_for_one, 5, 10}, [
			?WORKER(mongo_cursor, start_link, [], temporary)
		]
	}}.

-spec oid_local_id() -> <<_:40>>.
oid_local_id() ->
	OSPid = erlang:phash2({self(), list_to_integer(os:getpid())}, 65536),
	{ok, Hostname} = inet:gethostname(),
	<<MachineId:3/binary, _/binary>> = erlang:md5(Hostname),
	<<MachineId:3/binary, OSPid:16/big>>.

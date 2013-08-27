-module(mongo_client).
-export([
	start_link/4,
	stop/1,
	add_connection/2,
	get_connection/1
]).

-behaviour(gen_server).
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-export_type([
	id/0,
	host/0,
	options/0
]).

-record(state, {
	id      :: term(),
	options :: mongo_connection:options()
}).

-define(TAB, mongo).

-type id()      :: binary() | atom() | list() | tuple() | reference() | number().
-type host()    :: inet:hostname() | inet:ip_address().
-type options() :: [option()].
-type option()  :: {pool_size, pos_integer()} | mongo_connection:options().


-spec start_link(id(), host(), 1..65535, options()) -> {ok, pid()} | {error, term()}.
start_link(ClientId, Host, Port, Options) ->
	gen_server:start_link(?MODULE, [ClientId, Host, Port, Options], []).

-spec stop(id()) -> ok.
stop(ClientId) ->
	Pid = ets:lookup_element(?TAB, {client, ClientId}, 2),
	gen_server:call(Pid, stop).

%% @hidden
add_connection(ClientId, Connection) ->
	Pid = ets:lookup_element(?TAB, {client, ClientId}, 2),
	gen_server:cast(Pid, {add_connection, Connection}).

%% @hidden
get_connection(ClientId) ->
	Connections = ets:lookup_element(?TAB, {client, ClientId}, 3),
	Index = erlang:phash2(self(), length(Connections)),
	lists:nth(Index + 1, Connections).

%% @hidden
init([Id, Host, Port, Options]) ->
	case ets:insert_new(?TAB, {{client, Id}, self(), []}) of
		true ->
			PoolSize = proplists:get_value(pool_size, Options, 100),
			% TODO: filter the options list
			lists:foreach(fun(_) ->
				{ok, _} = mongo_sup:start_connection(Id, Host, Port, Options)
			end, lists:seq(1, PoolSize));
		false ->
			lists:foreach(fun(Connection) ->
				_ = erlang:monitor(process, Connection)
			end, ets:lookup_element(?TAB, {client, Id}, 3))
	end,
	{ok, #state{
		id = Id
	}}.

%% @hidden
handle_call(stop, _From, State) ->
	{stop, normal, ok, State};
handle_call(_Request, _From, State) ->
	{reply, ok, State}.

%% @hidden
handle_cast({add_connection, Connection}, State) ->
	Connections = ets:lookup_element(?TAB, {client, State#state.id}, 3),
	_ = erlang:monitor(process, Connection),
	true = ets:update_element(?TAB, {client, State#state.id}, {3, [Connection | Connections]}),
	{noreply, State};
handle_cast(_Request, State) ->
	{noreply, State}.

%% @hidden
handle_info({'DOWN', _, process, Connection, _}, State) ->
	Connections = ets:lookup_element(?TAB, {client, State#state.id}, 3),
	true = ets:update_element(?TAB, {client, State#state.id}, {3, lists:delete(Connection, Connections)}),
	{noreply, State};
handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, State) ->
	lists:foreach(fun(Connection) ->
		mongo_connection:stop(Connection)
	end, ets:lookup_element(?TAB, {client, State#state.id}, 3)),
	true = ets:delete(?TAB, {client, State#state.id}),
	ok.

%% @hidden
code_change(_Old, State, _Extra) ->
	{ok, State}.

-module(mongo_cursor).
-export([
	create/6,
	next/1,
	rest/1,
	take/2,
	foldl/4,
	map/3,
	close/1
]).

-export([
	start_link/7
]).

-export_type([
	cursor/0
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

-record(state, {
	connection  :: pid(),
	database    :: atom(),
	collection  :: atom(),
	cursor      :: integer(),
	batchsize   :: integer(),
	batch       :: [bson:document()],
	monitor     :: reference()
}).

-opaque cursor() :: pid().

-spec create(pid(), mongo_connection:database(), mongo_connection:collection(), integer(), integer(), [bson:document()]) -> {ok, cursor()}.
create(Connection, Database, Collection, Cursor, BatchSize, Batch) ->
	mongo_sup:start_cursor(self(), Connection, Database, Collection, Cursor, BatchSize, Batch).

-spec next(pid()) -> {} | {bson:document()}.
next(Cursor) ->
	try gen_server:call(Cursor, next, infinity) of
		Result -> Result
	catch
		exit:{noproc, _} -> {}
	end.

-spec rest(pid()) -> [bson:document()].
rest(Cursor) ->
	try gen_server:call(Cursor, rest, infinity) of
		Result -> Result
	catch
		exit:{noproc, _} -> []
	end.

-spec take(pid(), non_neg_integer()) -> [bson:document()].
take(Cursor, Limit) ->
	try gen_server:call(Cursor, {rest, Limit}, infinity) of
		Result -> Result
	catch
		exit:{noproc, _} -> []
	end.

-spec foldl(fun((bson:document(), term()) -> term()), term(), pid(), non_neg_integer()) -> term().
foldl(_Fun, Acc, _Cursor, 0) ->
	Acc;
foldl(Fun, Acc, Cursor, infinity) ->
	lists:foldl(Fun, Acc, rest(Cursor));
foldl(Fun, Acc, Cursor, Max) ->
	case next(Cursor) of
		{} -> Acc;
		{Doc} -> foldl(Fun, Fun(Doc, Acc), Cursor, Max - 1)
	end.

-spec map(fun((bson:document()) -> term()), pid(), non_neg_integer()) -> [term()].
map(Fun, Cursor, Max) ->
	lists:reverse(foldl(fun(Doc, Acc) ->
		[Fun(Doc) | Acc]
	end, [], Cursor, Max)).

-spec close(pid()) -> ok.
close(Cursor) ->
	catch gen_server:call(Cursor, stop),
	ok.

start_link(Owner, Connection, Database, Collection, Cursor, BatchSize, Batch) ->
	gen_server:start_link(?MODULE, [Owner, Connection, Database, Collection, Cursor, BatchSize, Batch], []).


%% @hidden
init([Owner, Connection, Database, Collection, Cursor, BatchSize, Batch]) ->
	{ok, #state{
		connection = Connection,
		database = Database,
		collection = Collection,
		cursor = Cursor,
		batchsize = BatchSize,
		batch = Batch,
		monitor = erlang:monitor(process, Owner)
	}}.

%% @hidden
handle_call(next, _From, State) ->
	case next_i(State) of
		{Reply, #state{cursor = 0, batch = []} = UpdatedState} ->
			{stop, normal, Reply, UpdatedState};
		{Reply, UpdatedState} ->
			{reply, Reply, UpdatedState}
	end;

handle_call(rest, _From, State) ->
	{Reply, UpdatedState} = rest_i(State, infinity),
	{stop, normal, Reply, UpdatedState};

handle_call({rest, Limit}, _From, State) ->
	case rest_i(State, Limit) of
		{Reply, #state{cursor = 0, batch = []} = UpdatedState} ->
			{stop, normal, Reply, UpdatedState};
		{Reply, UpdatedState} ->
			{reply, Reply, UpdatedState}
	end;

handle_call(stop, _From, State) ->
	{stop, normal, ok, State}.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info({'DOWN', Monitor, process, _, _}, #state{monitor = Monitor} = State) ->
	{stop, normal, State};

handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, #state{cursor = 0}) ->
	ok;
terminate(_, State) ->
	mongo_connection:kill_cursors(State#state.connection, [State#state.cursor], []).

%% @hidden
code_change(_Old, State, _Extra) ->
	{ok, State}.

%% @private
next_i(#state{batch = [Doc | Rest]} = State) ->
	{{Doc}, State#state{batch = Rest}};
next_i(#state{batch = [], cursor = 0} = State) ->
	{{}, State};
next_i(#state{batch = []} = State) ->
	{ok, Cursor, Batch} = mongo_connection:get_more(State#state.connection,
		State#state.database,
		State#state.collection,
		State#state.cursor,
		State#state.batchsize,
		[]
	),
	next_i(State#state{cursor = Cursor, batch = Batch}).

%% @private
rest_i(State, infinity) ->
	rest_i(State, -1);
rest_i(State, Limit) ->
	{Docs, UpdatedState} = rest_i(State, [], Limit),
	{lists:reverse(Docs), UpdatedState}.

%% @private
rest_i(State, Acc, 0) ->
	{Acc, State};
rest_i(State, Acc, Limit) ->
	case next_i(State) of
		{{}, UpdatedState} -> {Acc, UpdatedState};
		{{Doc}, UpdatedState} -> rest_i(UpdatedState, [Doc | Acc], Limit - 1)
	end.

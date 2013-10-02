-module(mongo).
-export([
	start_client/4,
	stop_client/1
]).
-export([
	do/5
]).
-export([
	insert/3,
	insert/2,
	update/4,
	update_all/4,
	update_one/4,
	delete/3,
	delete_all/2,
	delete_one/2
]).
-export([
	find_one/5,
	find_one/4,
	find_one/3,
	find_one/2,
	find_many/6,
	find_many/5,
	find_many/4,
	find_many/3,
	find_many/2,
	'query'/6
]).
-export([
	count/2,
	count/3
]).
-export([
	command/1
]).
-export([
	object_id/0
]).

-export_type([
	database/0,
	collection/0,

	read_error/0,
	write_error/0,

	insert_options/0,
	update_options/0,
	delete_options/0,
	find_options/0
]).

-type database() :: mongo_connection:database().
-type collection() :: mongo_connection:collection().
-type cursor() :: mongo_cursor:cursor().

-type read_mode()  :: mongo_connection:read_mode().
-type write_mode() :: mongo_connection:write_mode().

-type read_error()  :: mongo_connection:read_error().
-type write_error() :: mongo_connection:write_error().

-include("mongo.hrl").

-spec start_client(mongo_client:id(), mongo_client:host(), 1..65535, mongo_client:options()) -> ok.
start_client(Id, Host, Port, Options) ->
	{ok, _Pid} = mongo_sup:start_client(Id, Host, Port, Options),
	ok.

-spec stop_client(mongo_client:id()) -> ok.
stop_client(Id) ->
	mongo_client:stop(Id).

-spec do(read_mode(), write_mode(), mongo_client:id() | pid(), mongo:database(), action(R)) -> R.
-type action(R) :: fun(() -> R).
do(ReadMode, WriteMode, Connection, Database, Action) when is_pid(Connection) ->
	PrevContext = erlang:get(mongo_do_context),
	erlang:put(mongo_do_context, #context{
		connection = Connection,
		database = Database,
		read_mode = ReadMode,
		write_mode = WriteMode
	}),
	try Action() of
		Result -> Result
	after
		case PrevContext of
			undefined ->
				erlang:erase(mongo_do_context);
			_ ->
				erlang:put(mongo_do_context, PrevContext)
		end
	end;
do(ReadMode, WriteMode, ClientId, Database, Action) ->
	Connection = mongo_client:get_connection(ClientId),
	do(ReadMode, WriteMode, Connection, Database, Action).

-spec insert(collection(), bson:document() | [bson:document()], insert_options()) ->
		{ok, bson:document()} | {ok, [bson:document()]} | {error, write_error()}.
-type insert_options() :: mongo_connection:insert_options().
insert(Collection, Doc, Options) when is_tuple(hd(Doc)) ->
	case insert(Collection, [Doc], Options) of
		{ok, [Doc]} -> {ok, Doc};
		Error -> Error
	end;
insert(Collection, Docs, Options) ->
	#context{
		connection = Connection,
		database = Database,
		write_mode = WriteMode
	} = erlang:get(mongo_do_context),

	Docs1 = [ensure_object_id(Doc) || Doc <- Docs],
	case mongo_connection:insert(Connection, Database, Collection, Docs1, WriteMode, Options) of
		ok -> {ok, Docs1};
		{error, Error} -> {error, Error}
	end.

insert(Collection, Docs) ->
	insert(Collection, Docs, []).

-spec update(collection(), bson:document(), bson:document(), update_options()) ->
		ok | {error, write_error()}.
-type update_options() :: mongo_connection:update_options().
update(Collection, Selector, Updater, Options) ->
	#context{
		connection = Connection,
		database = Database,
		write_mode = WriteMode
	} = erlang:get(mongo_do_context),
	mongo_connection:update(Connection, Database, Collection, Selector, Updater, WriteMode, Options).

-spec update_all(collection(), bson:document(), bson:document(), boolean()) ->
		ok | {error, write_error()}.
update_all(Collection, Selector, Updater, Upsert) ->
	update(Collection, Selector, Updater, [multiple | case Upsert of true -> [upsert]; false -> [] end]).

-spec update_one(collection(), bson:document(), bson:document(), boolean()) ->
		ok | {error, write_error()}.
update_one(Collection, Selector, Updater, Upsert) ->
	update(Collection, Selector, Updater, case Upsert of true -> [upsert]; false -> [] end).

-spec delete(collection(), bson:document(), delete_options()) ->
		ok | {error, write_error()}.
-type delete_options() :: mongo_connection:delete_options().
delete(Collection, Selector, Options) ->
	#context{
		connection = Connection,
		database = Database,
		write_mode = WriteMode
	} = erlang:get(mongo_do_context),
	mongo_connection:delete(Connection, Database, Collection, Selector, WriteMode, Options).

delete_all(Collection, Selector) ->
	delete(Collection, Selector, []).

delete_one(Collection, Selector) ->
	delete(Collection, Selector, [single]).

-spec find_one(collection(), bson:document(), undefined | bson:document(), non_neg_integer(), find_options()) ->
		{ok, bson:document()} | {error, not_found | read_error()}.
-type find_options() :: mongo_connection:query_options().
find_one(Collection, Selector, Projector, Skip, Options) ->
	#context{
		connection = Connection,
		database = Database,
		read_mode = ReadMode
	} = erlang:get(mongo_do_context),
	case mongo_connection:'query'(Connection, Database, Collection, Selector, Projector, Skip, -1, ReadMode, Options) of
		{ok, 0, []} -> {error, not_found};
		{ok, 0, [Doc]} -> {ok, Doc};
		Error -> Error
	end.

find_one(Collection, Selector, Projector, Skip) ->
	find_one(Collection, Selector, Projector, Skip, []).

find_one(Collection, Selector, Projector) ->
	find_one(Collection, Selector, Projector, 0, []).

find_one(Collection, Selector) ->
	find_one(Collection, Selector, undefined, 0, []).

-spec find_many(collection(), bson:document(), undefined | bson:document(), non_neg_integer(), pos_integer() | infinity, find_options()) ->
		{ok, [bson:document()]} | {error, read_error()}.
find_many(Collection, Selector, Projector, Skip, Limit, Options) when is_integer(Limit), Limit > 0 ->
	#context{
		connection = Connection,
		database = Database,
		read_mode = ReadMode
	} = erlang:get(mongo_do_context),
	case mongo_connection:'query'(Connection, Database, Collection, Selector, Projector, Skip, -Limit, ReadMode, Options) of
		{ok, 0, Documents} -> {ok, Documents};
		Error -> Error
	end;
find_many(Collection, Selector, Projector, Skip, infinity, Options) ->
	#context{
		connection = Connection,
		database = Database,
		read_mode = ReadMode
	} = erlang:get(mongo_do_context),
	case mongo_connection:'query'(Connection, Database, Collection, Selector, Projector, Skip, 0, ReadMode, Options) of
		{ok, Cursor, Documents} ->
			drain_cursor(Connection, Database, Collection, Cursor, Documents);
		Error ->
			Error
	end.

drain_cursor(_Connection, _Database, _Collection, 0, Acc) ->
	{ok, Acc};
drain_cursor(Connection, Database, Collection, Cursor, Acc) ->
	case mongo_connection:get_more(Connection, Database, Collection, Cursor, 0, []) of
		{ok, Cursor1, Batch} ->
			drain_cursor(Connection, Database, Collection, Cursor1, Acc ++ Batch);
		Error ->
			Error
	end.

find_many(Collection, Selector, Projector, Skip, Limit) ->
	find_many(Collection, Selector, Projector, Skip, Limit, []).

find_many(Collection, Selector, Projector, Skip) ->
	find_many(Collection, Selector, Projector, Skip, infinity, []).

find_many(Collection, Selector, Projector) ->
	find_many(Collection, Selector, Projector, 0, infinity, []).

find_many(Collection, Selector) ->
	find_many(Collection, Selector, undefined, 0, infinity, []).

-spec 'query'(collection(), bson:document(), undefined | bson:document(), non_neg_integer(), integer(), find_options()) ->
		{ok, cursor()} | {error, read_error()}.
'query'(Collection, Selector, Projector, Skip, Count, Options) ->
	#context{
		connection = Connection,
		database = Database,
		read_mode = ReadMode
	} = erlang:get(mongo_do_context),
	case mongo_connection:'query'(Connection, Database, Collection, Selector, Projector, Skip, Count, ReadMode, Options) of
		{ok, Cursor, Batch} ->
			mongo_cursor:create(Connection, Database, Collection, Cursor, Count, Batch);
		Error ->
			Error
	end.

-spec count(collection(), bson:document()) -> {ok, non_neg_integer()} | {error, read_error()}.
count(Collection, Selector) ->
	count(Collection, Selector, 0).

-spec count(collection(), bson:document(), integer()) -> {ok, non_neg_integer()} | {error, read_error()}.
count(Collection, Selector, Limit) ->
	case command(if
		Limit =< 0 -> [{<<"count">>, to_binary(Collection)}, {<<"query">>, Selector}];
		Limit > 0 -> [{<<"count">>, to_binary(Collection)}, {<<"query">>, Selector}, {<<"limit">>, Limit}]
	end) of
		{ok, Reply} -> {ok, trunc(bson:at(<<"n">>, Reply))};
		Error -> Error
	end.

-spec command(bson:document()) -> {ok, bson:document()} | {error, {bad_command, bson:document()} | read_error()}.
command(Command) ->
	case find_one(<<"$cmd">>, Command) of
		{ok, Doc} ->
			case bson:at(<<"ok">>, Doc) of
				true -> {ok, Doc};
				N when N == 1 -> {ok, Doc};
				_ -> {error, {bad_command, Doc}}
			end;
		Error ->
			Error
	end.

-spec object_id() -> bson:object_id().
object_id() ->
	LocalId = ets:lookup_element(mongo, oid_local_id, 2),
	Counter = ets:update_counter(mongo, oid_counter, 1),
	bson:object_id(os:timestamp(), LocalId, Counter).


%% @private
ensure_object_id(Doc) ->
	case bson:lookup(<<"_id">>, Doc) of
		{ok, _Value} -> Doc;
		undefined -> bson:update(<<"_id">>, mongo:object_id(), Doc)
	end.

%% @private
to_binary(Value) when is_integer(Value) ->
	list_to_binary(integer_to_list(Value));
to_binary(Value) when is_atom(Value) ->
	atom_to_binary(Value, utf8);
to_binary(Value) when is_binary(Value) ->
	Value;
to_binary(_Value) ->
	<<>>.

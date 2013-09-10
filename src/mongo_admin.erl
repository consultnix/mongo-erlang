-module(mongo_admin).
-export([
	ensure_index/3,
	ensure_index/2
]).
-export([
	drop_collection/1,
	drop_database/0
]).

-include("mongo.hrl").

-spec ensure_index(mongo:collection(), bson:document(), [bson:field() | bson:label()]) ->
		ok | {error, mongo:write_error()}.
ensure_index(Collection, Key, Options) ->
	ensure_index_i(Collection, bson:update(<<"key">>, Key, lists:foldl(fun
		({Option, Value}, Acc) -> bson:update(Option, Value, Acc);
		(Option, Acc) when is_atom(Option); is_binary(Option) -> bson:update(Option, true, Acc);
		({}, Acc) -> Acc
	end, bson:new(), Options))).

ensure_index(Collection, Key) ->
	ensure_index(Collection, Key, []).

-spec drop_collection(mongo:collection()) -> ok | {error, {bad_command, bson:document()} | mongo:read_error()}.
drop_collection(Collection) ->
	case mongo:command([{<<"drop">>, Collection}]) of
		{ok, _} ->
			ok;
		{error, {bad_command, Error}} ->
			case bson:at(<<"errmsg">>, Error) of
				<<"ns not found">> ->
					ok;
				_ ->
					{error, {bad_command, Error}}
			end;
		{error, Error} ->
			{error, Error}
	end.

-spec drop_database() -> ok | {error, mongo:read_error()}.
drop_database() ->
	case mongo:command([{<<"dropDatabase">>, 1}]) of
		{ok, _} ->
			ok;
		{error, Error} ->
			{error, Error}
	end.


%% @private
ensure_index_i(Collection, Index) ->
	Defaults = [
		{<<"name">>, index_name(bson:at(<<"key">>, Index))},
		{<<"unique">>, false},
		{<<"dropDups">>, false}
	],
	#context{database = Database} = erlang:get(mongo_do_context),
	Namespace = <<(to_binary(Database))/binary, $., (to_binary(Collection))/binary>>,
	case mongo:insert('system.indexes', [bson:update(<<"ns">>, Namespace, bson:merge(Index, Defaults))]) of
		{ok, _} -> ok;
		Error -> Error
	end.

%% @private
index_name(KeyOrder) ->
	lists:foldl(fun({Label, Order}, Acc) ->
		<<Acc/binary, $_, (to_binary(Label))/binary, $_, (to_binary(Order))/binary>>
	end, <<"i">>, KeyOrder).

%% @private
to_binary(Value) when is_integer(Value) ->
	list_to_binary(integer_to_list(Value));
to_binary(Value) when is_atom(Value) ->
	atom_to_binary(Value, utf8);
to_binary(Value) when is_binary(Value) ->
	Value;
to_binary(_Value) ->
	<<>>.

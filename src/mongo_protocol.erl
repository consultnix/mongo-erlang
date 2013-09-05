-module(mongo_protocol).
-export([
	encode_message/2,
	decode_message/1
]).

-include("mongo_protocol.hrl").
-define(INT8(B), (B):8/little).
-define(INT32(I), (I):32/little-signed).
-define(INT64(I), (I):64/little-signed).
-define(NULL, ?INT8(0)).

-define(OP_REPLY, 1).
-define(OP_UPDATE, 2001).
-define(OP_INSERT, 2002).
-define(OP_QUERY, 2004).
-define(OP_GET_MORE, 2005).
-define(OP_DELETE, 2006).
-define(OP_KILL_CURSORS, 2007).

-define(HEADER(Id, ResponseTo, Op), ?INT32(Id), ?INT32(ResponseTo), ?INT32(Op)).
-define(DB_COLL(Db, Coll), (to_binary(Db))/binary, $., (to_binary(Coll))/binary, ?NULL).


-spec encode_message(request_id(), message()) -> binary().
encode_message(Id, #update{database = Db, collection = Coll, multiple = Multi, upsert = Upsert, selector = Selector, updater = Updater}) ->
	<<
		?HEADER(Id, 0, ?OP_UPDATE),
		?INT32(0),
		?DB_COLL(Db, Coll),
		0:6, (bit(Multi)):1, (bit(Upsert)):1, 0:24,
		(bson:encode(Selector))/binary,
		(bson:encode(Updater))/binary
	>>;
encode_message(Id, #insert{database = Db, collection = Coll, documents = Docs, continue_on_error = ContinueOnError}) ->
	<<
		?HEADER(Id, 0, ?OP_INSERT),
		0:7, (bit(ContinueOnError)):1, 0:24,
		?DB_COLL(Db, Coll),
		<< <<(bson:encode(Doc))/binary>> || Doc <- Docs >>/binary
	>>;
encode_message(Id, #'query'{} = Message) ->
	#'query'{
		database = Db,
		collection = Coll,
		selector = Selector,
		projector = Projector,
		skip = Skip,
		count = Count,
		tailable = Tailable,
		slave_ok = SlaveOk,
		no_cursor_timeout = NoCursorTimeout,
		await_data = AwaitData,
		exhaust = Exhaust,
		partial = Partial
	} = Message,
	<<?HEADER(Id, 0, ?OP_QUERY),
		(bit(Partial)):1, (bit(Exhaust)):1, (bit(AwaitData)):1, (bit(NoCursorTimeout)):1, 0:1, (bit(SlaveOk)):1, (bit(Tailable)):1, 0:1, 0:24,
		?DB_COLL(Db, Coll),
		?INT32(Skip),
		?INT32(Count),
		(bson:encode(Selector))/binary,
		(case Projector of
			undefined -> <<>>;
			_ -> bson:encode(Projector)
		end)/binary
	>>;
encode_message(Id, #get_more{database = Db, collection = Coll, count = Count, cursor_id = CursorId}) ->
	<<
		?HEADER(Id, 0, ?OP_GET_MORE),
		?INT32(0),
		?DB_COLL(Db, Coll),
		?INT32(Count),
		?INT64(CursorId)
	>>;
encode_message(Id, #delete{database = Db, collection = Coll, selector = Selector, single = Single}) ->
	<<
		?HEADER(Id, 0, ?OP_DELETE),
		?INT32(0),
		?DB_COLL(Db, Coll),
		0:7, (bit(Single)):1, 0:24,
		(bson:encode(Selector))/binary
	>>;
encode_message(Id, #kill_cursors{cursor_ids = Ids}) ->
	<<
		?HEADER(Id, 0, ?OP_KILL_CURSORS),
		?INT32(0),
		?INT32(length(Ids)),
		<< <<?INT64(CursorId)>> || CursorId <- Ids >>/binary
	>>;
encode_message(Id, #reply{} = Message) ->
	#reply{
		response_to = ResponseTo,

		cursor_not_found = CursorNotFound,
		query_error = QueryError,
		await_capable = AwaitCapable,

		cursor_id = CursorId,
		offset = Offset,
		documents = Documents
	} = Message,
	<<
		?HEADER(Id, ResponseTo, ?OP_REPLY),
		0:4, (bit(AwaitCapable)):1, 0:1, (bit(QueryError)):1, (bit(CursorNotFound)):1, 0:24,
		?INT64(CursorId),
		?INT32(Offset),
		?INT32(length(Documents)),
		<< <<(bson:encode(Doc))/binary>> || Doc <- Documents >>/binary
	>>.

-spec decode_message(binary()) -> {request_id(), message(), binary()}.
decode_message(<<?HEADER(Id, ResponseTo, ?OP_REPLY), Data/binary>>) ->
	<<
		_:4, AwaitCapable:1, _:1, QueryError:1, CursorNotFound:1, _:24,
		?INT64(CursorId),
		?INT32(Offset),
		?INT32(Count),
		Rest1/binary
	>> = Data,
	{Docs, Rest2} = decode_documents(Count, Rest1, []),
	{Id, #reply {
		response_to = ResponseTo,
		cursor_not_found = bool(CursorNotFound),
		query_error = bool(QueryError),
		await_capable = bool(AwaitCapable),
		cursor_id = CursorId,
		offset = Offset,
		documents = Docs
	}, Rest2}.


%% @private
decode_documents(0, Data, Acc) ->
	{lists:reverse(Acc), Data};
decode_documents(Count, Data, Acc) ->
	{Doc, Rest} = bson:decode(Data),
	decode_documents(Count - 1, Rest, [Doc | Acc]).

%% @private
to_binary(Bin) when is_binary(Bin) ->
	Bin;
to_binary(Atom) when is_atom(Atom) ->
	atom_to_binary(Atom, utf8).

%% @private
bit(false) -> 0;
bit(true) -> 1.

%% @private
bool(0) -> false;
bool(1) -> true.

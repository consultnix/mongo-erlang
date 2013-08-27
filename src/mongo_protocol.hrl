-type request_id() :: integer().
-type cursor_id() :: integer().

-record(update, {
	database   :: mongo:database(),
	collection :: mongo:collection(),
	selector   :: bson:document(),
	updater    :: bson:document(),

	multiple = false :: boolean(),
	upsert = false :: boolean()
}).

-record(insert, {
	database   :: mongo:database(),
	collection :: mongo:collection(),
	documents  :: [bson:document()],

	% Flags
	continue_on_error = false :: boolean()
}).

-record('query', {
	database   :: mongo:database(),
	collection :: mongo:collection(),
	selector   :: bson:document(),
	projector  :: undefined | bson:document(),
	skip = 0   :: non_neg_integer(),
	count      :: integer(),

	% Flags
	tailable = false :: boolean(),
	slave_ok = false :: boolean(),
	no_cursor_timeout = false :: boolean(),
	await_data = false :: boolean(),
	exhaust = false :: boolean(),
	partial = false :: boolean()
}).

-record(get_more, {
	database   :: mongo:database(),
	collection :: mongo:collection(),
	count      :: integer(),
	cursor_id  :: cursor_id()
}).

-record(delete, {
	database   :: mongo:database(),
	collection :: mongo:collection(),
	selector   :: bson:document(),
	single = false :: boolean()
}).

-record(kill_cursors, {
	cursor_ids :: [cursor_id()]
}).

-record(reply, {
	response_to :: request_id(),

	cursor_not_found :: boolean(),
	query_error :: boolean(),
	await_capable :: boolean(),

	cursor_id :: cursor_id(),
	offset :: non_neg_integer(),
	documents :: [bson:document()]
}).

-type notice() :: #insert{} | #update{} | #delete{} | #kill_cursors{}.
-type request() :: #'query'{} | #get_more{}.
-type reply() :: #reply{}.
-type message() :: notice() | request() | reply().

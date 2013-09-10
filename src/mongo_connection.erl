-module(mongo_connection).
-export([
	start_link/4,
	stop/1
]).

-export([
	update/7,
	insert/6,
	'query'/9,
	get_more/6,
	delete/6,
	kill_cursors/3
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
	host/0,
	options/0,
	database/0,
	collection/0,

	read_mode/0,
	write_mode/0,

	read_error/0,
	write_error/0,

	update_options/0,
	insert_options/0,
	query_options/0,
	get_more_options/0,
	delete_options/0,
	kill_cursors_options/0
]).

-record(state, {
	transport    :: gen_tcp | ssl,
	socket       :: port() | ssl:sslsocket(),
	buffer       :: binary(),

	requests     :: [{non_neg_integer(), {pid(), term()}}],
	next_id      :: non_neg_integer()
}).

-type host()     :: inet:hostname() | inet:ip_address().
-type options()  :: [option()].
-type option()   :: {connect_timeout, timeout()} |
		{ssl, boolean()} | ssl |
		{keepalive, boolean()} | keepalive.
-type database() :: atom() | binary().
-type collection() :: atom() | binary().
-type read_mode() :: master | slave_ok.
-type write_mode() :: safe | {safe, bson:document()} | unsafe.
-type read_error()  :: not_master | not_authorized | {bad_query, bson:document()}.
-type write_error() :: not_master | {duplicate_key, Message :: binary()} | {write_failure, Code :: non_neg_integer(), Message :: binary()}.

-include("mongo_protocol.hrl").

-spec start_link(mongo_client:id(), host(), 1..65535, options()) ->
		{ok, pid()}.
start_link(Client, Host, Port, Options) ->
	{ok, Connection} = gen_server:start_link(?MODULE, [Host, Port, Options], []),
	ok = mongo_client:add_connection(Client, Connection),
	{ok, Connection}.

-spec update(pid(), database(), collection(), bson:document(), bson:document(), write_mode(), update_options()) ->
		ok | {error, write_error()}.
-type update_options() :: [update_option()].
-type update_option() :: multiple | upsert.
update(Pid, Database, Collection, Selector, Updater, WriteMode, Options) ->
	write(Pid, Database, #update{
		database = Database,
		collection = Collection,
		selector = Selector,
		updater = Updater,
		multiple = proplists:get_bool(multiple, Options),
		upsert = proplists:get_bool(upsert, Options)
	}, WriteMode, Options).

-spec insert(pid(), database(), collection(), [bson:document()], write_mode(), insert_options()) ->
		ok | {error, write_error()}.
-type insert_options() :: [insert_option()].
-type insert_option()  :: continue_on_error.
insert(Pid, Database, Collection, Documents, WriteMode, Options) ->
	write(Pid, Database, #insert{
		database = Database,
		collection = Collection,
		documents = Documents,
		continue_on_error = proplists:get_bool(continue_on_error, Options)
	}, WriteMode, Options).

-spec 'query'(pid(), database(), collection(), bson:document(), projector(), non_neg_integer(), integer(), read_mode(), query_options()) ->
		{ok, cursor_id(), [bson:document()]} | {error, read_error()}.
-type projector() :: bson:document() | undefined.
-type query_options() :: [query_option()].
-type query_option() :: tailable | no_cursor_timeout | exhaust | partial.
'query'(Pid, Database, Collection, Selector, Projector, Skip, Count, ReadMode, Options) ->
	read(Pid, Database, #'query'{
		database = Database,
		collection = Collection,
		selector = Selector,
		projector = Projector,
		skip = Skip,
		count = Count,
		tailable = proplists:get_bool(tailable, Options),
		slave_ok = case ReadMode of master -> false; slave_ok -> true end,
		no_cursor_timeout = proplists:get_bool(no_cursor_timeout, Options),
		await_data = proplists:get_bool(tailable, Options),
		exhaust = proplists:get_bool(exhaust, Options),
		partial = proplists:get_bool(partial, Options)
	}, Options).

-spec get_more(pid(), database(), collection(), integer(), cursor_id(), get_more_options()) ->
		{ok, cursor_id(), [bson:document()]} | {error, read_error()}.
-type get_more_options() :: [].
get_more(Pid, Database, Collection, CursorId, Count, Options = []) ->
	read(Pid, Database, #get_more{
		database = Database,
		collection = Collection,
		cursor_id = CursorId,
		count = Count
	}, Options).

-spec delete(pid(), database(), collection(), bson:document(), write_mode(), delete_options()) ->
		ok | {error, write_error()}.
-type delete_options() :: [delete_option()].
-type delete_option() :: single.
delete(Pid, Database, Collection, Selector, WriteMode, Options) ->
	write(Pid, Database, #delete{
		database = Database,
		collection = Collection,
		selector = Selector,
		single = proplists:get_bool(single, Options)
	}, WriteMode, Options).

-spec kill_cursors(pid(), [cursor_id()], kill_cursors_options()) -> ok.
-type kill_cursors_options() :: [].
kill_cursors(Pid, Cursors, []) ->
	gen_server:cast(Pid, {notice, #kill_cursors{
		cursor_ids = Cursors
	}}).

-spec stop(pid()) -> ok.
stop(Pid) ->
	gen_server:call(Pid, stop).


%% @hidden
init([Host, Port, Options]) ->
	Timeout = proplists:get_value(connect_timeout, Options, infinity),
	Transport = case proplists:get_bool(ssl, Options) of true -> ssl; false -> gen_tcp end,
	KeepAlive = proplists:get_bool(keepalive, Options),
	case Transport:connect(Host, Port, [binary, {active, once}, {packet, raw}, {keepalive, KeepAlive}], Timeout) of
		{ok, Socket} ->
			{ok, #state{
				transport = Transport,
				socket = Socket,
				buffer = <<>>,
				requests = [],
				next_id = 1
			}};
		{error, Reason} ->
			{stop, Reason}
	end.

%% @hidden
handle_call({read, Query}, From, #state{next_id = Id} = State) ->
	case (State#state.transport):send(State#state.socket, encode(Id, Query)) of
		ok ->
			{noreply, State#state{
				requests = [{Id, From} | State#state.requests],
				next_id = Id + 1
			}};
		{error, Reason} ->
			{stop, Reason, State}
	end;
handle_call({write, Database, Update, Safe}, From, #state{next_id = Id} = State) ->
	Payload = [
		encode(Id, Update),
		encode(Id + 1, #'query'{
			database = Database,
			collection = <<"$cmd">>,
			selector = bson:update(<<"getlasterror">>, 1, Safe),
			count = -1
		})
	],
	case (State#state.transport):send(State#state.socket, Payload) of
		ok ->
			{noreply, State#state{
				requests = [{Id + 1, From} | State#state.requests],
				next_id = Id + 2
			}};
		{error, Reason} ->
			{stop, Reason, State}
	end;
handle_call(stop, _From, State) ->
	{stop, normal, ok, State};
handle_call(_, _From, State) ->
	{reply, ignored, State}.

%% @hidden
handle_cast({notice, Notice}, #state{next_id = Id} = State) ->
	case (State#state.transport):send(State#state.socket, encode(Id, Notice)) of
		ok ->
			{noreply, State#state{next_id = Id + 1}};
		{error, Reason} ->
			{stop, Reason, State}
	end;
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info({Tag, Socket, Data}, #state{transport = Transport, socket = Socket} = State) when Tag =:= tcp; Tag =:= ssl ->
	Buffer = <<(State#state.buffer)/binary, Data/binary>>,
	{Messages, Rest} = decode(Buffer),
	ok = case Transport of
		gen_tcp -> inet:setopts(State#state.socket, [{active, once}]);
		ssl -> ssl:setopts(State#state.socket, [{active, once}])
	end,
	{noreply, State#state{
		requests = process_responses(Messages, State#state.requests),
		buffer = Rest
	}};

handle_info({Tag, _Socket}, State) when Tag =:= tcp_closed; Tag =:= ssl_closed ->
	{stop, tcp_closed, State};

handle_info({Tag, _Socket, Reason}, State) when Tag =:= tcp_error; Tag =:= ssl_error ->
	{stop, Reason, State};

handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, #state{transport = Transport, socket = Socket}) ->
	catch Transport:close(Socket),
	ok.

%% @hidden
code_change(_Old, State, _Extra) ->
	{ok, State}.


%% @private
write(Pid, _Database, Update, unsafe, _Options) ->
	gen_server:cast(Pid, {notice, Update});
write(Pid, Database, Update, safe, Options) ->
	write(Pid, Database, Update, {safe, [{}]}, Options);
write(Pid, Database, Update, {safe, Safe}, _Options) ->
	#reply{documents = [Doc | _]} = gen_server:call(Pid, {write, Database, Update, Safe}),
	case bson:lookup([<<"err">>], Doc) of
		undefined ->
			ok;
		{ok, Message} when Message =:= undefined; Message =:= null ->
			ok;
		{ok, Message} ->
			% TODO: support more error codes
			case bson:at([<<"code">>], Doc) of
				10058 -> {error, not_master};
				Code when Code =:= 11000; Code =:= 11001 -> {error, {duplicate_key, Message}};
				Code -> {error, {write_failure, Code, Message}}
			end
	end.

%% @private
read(Pid, _Database, Query, _Options) ->
	case gen_server:call(Pid, {read, Query}) of
		#reply{cursor_not_found = false, query_error = false} = Reply ->
			{ok, Reply#reply.cursor_id, Reply#reply.documents};
		#reply{cursor_not_found = false, query_error = true, documents = [Doc | _]}  ->
			{error, case bson:at([<<"code">>], Doc) of
				% TODO: support more error codes
				13435 -> not_master;
				10057 -> not_authorized;
				_ -> {bad_query, Doc}
			end};
		#reply{cursor_not_found = true, query_error = false} ->
			{error, cursor_not_found}
	end.


%% @private
encode(Id, Request) ->
	Payload = mongo_protocol:encode_message(Id, Request),
	<<(byte_size(Payload) + 4):32/little, Payload/binary>>.

%% @private
decode(Data) ->
	decode(Data, []).

%% @private
decode(<<Length:32/signed-little, Data/binary>>, Acc) when byte_size(Data) >= (Length - 4) ->
	PayloadLength = Length - 4,
	<<Payload:PayloadLength/binary, Rest/binary>> = Data,
	{_, Message, <<>>} = mongo_protocol:decode_message(Payload),
	decode(Rest, [{Message#reply.response_to, Message} | Acc]);
decode(Data, Acc) ->
	{lists:reverse(Acc), Data}.

%% @private
process_responses([], Requests) ->
	Requests;
process_responses([{Id, Reply} | Replies], Requests) ->
	case lists:keytake(Id, 1, Requests) of
		false ->
			process_responses(Replies, Requests);
		{value, {_Id, From}, RemainingRequests} ->
			gen_server:reply(From, Reply),
			process_responses(Replies, RemainingRequests)
	end.

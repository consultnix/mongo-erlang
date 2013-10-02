-module(mongo_SUITE).

-include_lib("common_test/include/ct.hrl").
-export([
	all/0,
	init_per_suite/1,
	end_per_suite/1,
	init_per_testcase/2,
	end_per_testcase/2
]).


-export([
	insert_and_find/1,
	insert_and_delete/1
]).


all() ->
	[insert_and_find, insert_and_delete].

init_per_suite(Config) ->
	ok = application:start(mongo),
	ok = mongo:start_client(?MODULE, "127.0.0.1", 27017, []),
	[{database, test} | Config].

end_per_suite(_Config) ->
	ok = mongo:stop_client(?MODULE),
	application:stop(mongo),
	ok.

init_per_testcase(Case, Config) ->
	[{collection, collection(Case)} | Config].

end_per_testcase(_Case, Config) ->
	Database   = ?config(database, Config),
	Collection = ?config(collection, Config),
	ok = mongo:do(master, safe, ?MODULE, Database, fun() ->
		mongo_admin:drop_collection(Collection)
	end),
	ok.

%% Tests
insert_and_find(Config) ->
	Database   = ?config(database, Config),
	Collection = ?config(collection, Config),
	mongo:do(master, safe, ?MODULE, Database, fun () ->
		{ok, []} = mongo:find_many(Collection, [{}], undefined, 0, 10),
		{ok, Teams} = mongo:insert(Collection, [
			[{<<"name">>, <<"Yankees">>}, {<<"home">>, [{<<"city">>, <<"New York">>}, {<<"state">>, <<"NY">>}]}, {<<"league">>, <<"American">>}],
			[{<<"name">>, <<"Mets">>}, {<<"home">>, [{<<"city">>, <<"New York">>}, {<<"state">>, <<"NY">>}]}, {<<"league">>, <<"National">>}],
			[{<<"name">>, <<"Phillies">>}, {<<"home">>, [{<<"city">>, <<"Philadelphia">>}, {<<"state">>, <<"PA">>}]}, {<<"league">>, <<"National">>}],
			[{<<"name">>, <<"Red Sox">>}, {<<"home">>, [{<<"city">>, <<"Boston">>}, {<<"state">>, <<"MA">>}]}, {<<"league">>, <<"American">>}]
		]),
		{ok, 4} = mongo:count(Collection, [{}]),
		{ok, Teams} = mongo:find_many(Collection, [{}]),

		NationalTeams = [Team || Team <- Teams, bson:at(<<"league">>, Team) == <<"National">>],
		{ok, NationalTeams} = mongo:find_many(Collection, [{<<"league">>, <<"National">>}], undefined, 0, 10),
		{ok, 2} = mongo:count(Collection, [{<<"league">>, <<"National">>}]),

		TeamNames = [[{<<"name">>, bson:at(<<"name">>, Team)}] || Team <- Teams],
		{ok, TeamNames} = mongo:find_many(Collection, [{}], [{<<"_id">>, 0}, {<<"name">>, 1}], 0, 10),


		BostonTeam = lists:last(Teams),
		{ok, BostonTeam} = mongo:find_one(Collection, [{<<"home">>, [{<<"city">>, <<"Boston">>}, {<<"state">>, <<"MA">>}]}])
	end).

insert_and_delete(Config) ->
	Database   = ?config(database, Config),
	Collection = ?config(collection, Config),
	mongo:do(master, safe, ?MODULE, Database, fun () ->
		_Teams = mongo:insert(Collection, [
			[{<<"name">>, <<"Yankees">>}, {<<"home">>, [{<<"city">>, <<"New York">>}, {<<"state">>, <<"NY">>}]}, {<<"league">>, <<"American">>}],
			[{<<"name">>, <<"Mets">>}, {<<"home">>, [{<<"city">>, <<"New York">>}, {<<"state">>, <<"NY">>}]}, {<<"league">>, <<"National">>}],
			[{<<"name">>, <<"Phillies">>}, {<<"home">>, [{<<"city">>, <<"Philadelphia">>}, {<<"state">>, <<"PA">>}]}, {<<"league">>, <<"National">>}],
			[{<<"name">>, <<"Red Sox">>}, {<<"home">>, [{<<"city">>, <<"Boston">>}, {<<"state">>, <<"MA">>}]}, {<<"league">>, <<"American">>}]
		]),
		{ok, 4} = mongo:count(Collection, [{}]),

		mongo:delete_one(Collection, [{}]),
		{ok, 3} = mongo:count(Collection, [{}])
	end).

%% @private
collection(Case) ->
	Now = now_to_seconds(erlang:now()),
	list_to_binary(atom_to_list(?MODULE) ++ "-" ++ atom_to_list(Case) ++ "-" ++ integer_to_list(Now)).

%% @private
now_to_seconds({Mega, Sec, _}) ->
	(Mega * 1000000) + Sec.


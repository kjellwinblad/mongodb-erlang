-module(my_test).

-export([ main/1 ]).

show(Type, Val) ->
    erlang:display({Type, Val}),
    Val.

main(_) ->
    application:ensure_all_started(mongodb),
    application:set_env(mongodb, use_legacy_protocol, false),
    show(use_legacy_protocol,application:get_env(mongodb, use_legacy_protocol)),
    Database = <<"db">>,
    {ok, Connection} = mc_worker_api:connect ([{database, Database}]),
    Collection = <<"test_coll">>,
    %%timer:sleep(10000),
    show(insert3,mc_worker_api:insert(Connection, Collection, [#{<<"_id">> => 3}])),
    show(insert3,mc_worker_api:insert(Connection, Collection, [#{<<"_id">> => 4}])),
    show(insert3,mc_worker_api:insert(Connection, Collection, [#{<<"_id">> => 5}])),
    show(insert3again,mc_worker_api:insert(Connection, Collection, [#{<<"_id">> => 3}])),
    show(update,mc_worker_api:update(Connection, Collection, #{<<"_id">> => 5}, #{<<"_id">> => 5, <<"hej">> => 42})),
    show(delete_one_not_existing,mc_worker_api:delete_one(Connection, Collection, #{<<"_id">> => 42})),
    show(delete_one_3, mc_worker_api:delete_one(Connection, Collection, #{<<"_id">> => 3})),
    show(command_test,mc_worker_api:command(Connection, [{<<"find">>, Collection}])),
    show(find_one_new,mc_worker_api:find_one(Connection, Collection, #{<<"_id">> => 5})),
    show(find_one_ne_new,mc_worker_api:find_one(Connection, Collection, #{<<"_id">> => 10})),
    application:set_env(mongodb, use_legacy_protocol, true),
    show(find_one_ne_old,mc_worker_api:find_one(Connection, Collection, #{<<"_id">> => 10})),
    show(find_one_old,mc_worker_api:find_one(Connection, Collection, #{<<"_id">> => 5})),
    show(command_test_leg,mc_worker_api:command(Connection, [{<<"find">>, Collection}])).

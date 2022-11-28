-module(my_test).

-export([ main/1 ]).



main(_) ->
    application:ensure_all_started(mongodb),
    Database = <<"db">>,
    {ok, Connection} = mc_worker_api:connect ([{database, Database}]),
    Collection = <<"test_coll">>,
    %%timer:sleep(10000),
    mc_worker_api:insert(Connection, Collection, [
                                                  #{<<"_id">> => 3}]).

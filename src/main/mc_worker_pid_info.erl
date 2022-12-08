-module(mc_worker_pid_info).

%% About
%%
%% Module that help users get information (such as the protocol type) ragaring
%% mc_worker PIDs

-behaviour(gen_server).

-export([start_link/0,
         init/1,
         terminate/2,
         handle_cast/2,
         handle_call/3,
         get_info/1,
         set_info/2,
         discard_info/1,
         get_protocol_type/1]).


-define(MC_WORKER_PID_INFO_TAB_NAME, mc_worker_pid_info_tab).


-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    ets:new(?MC_WORKER_PID_INFO_TAB_NAME, [public, named_table, {read_concurrency, true}]),
    {ok, #{}}.

terminate(_,_) ->
    ets:delete(?MC_WORKER_PID_INFO_TAB_NAME).


%% These functions does not do anyting as this server is just a holder of an
%% ETS table
handle_cast(_Request, _State) -> {noreply, #{}}.
handle_call(_Request, _From, _State) -> {reply, ok, #{}}.

get_info(MCWorkerPID) ->
    try
        case ets:lookup(?MC_WORKER_PID_INFO_TAB_NAME, MCWorkerPID) of
            [{MCWorkerPID, InfoMap}] -> 
                {ok, InfoMap};
            [] ->
                not_found
        end
    catch
        _:_:_ ->
            not_found
    end.


set_info(MCWorkerPID, InfoMap) ->
    ets:insert(?MC_WORKER_PID_INFO_TAB_NAME, {MCWorkerPID, InfoMap}).

discard_info(MCWorkerPID) ->
    ets:delete(?MC_WORKER_PID_INFO_TAB_NAME, MCWorkerPID).

get_protocol_type(MCWorkerPID) ->
    case get_info(MCWorkerPID) of
        {ok, #{protocol_type := ProtocolType}} ->
            ProtocolType;
        _ ->
            %% Not found means that this library has been hot upgraded and the
            %% mc_worker process was created before the hot_upgrade so we use
            %% the legacy protocol as this was what existed before
            legacy
    end.


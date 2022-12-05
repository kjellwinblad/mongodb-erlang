%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jul 2016 15:12
%%%-------------------------------------------------------------------
-module(mc_selecting_logics).
-author("tihon").

-include("mongoc.hrl").

-define(SUITABLE_TYPE(T), T =:= rsSecondary; T =:= mongos; T =:= standalone).

%% API
-export([select_server/3]).

select_server(Topology, primaryPreferred, Tags) ->
    erlang:display({primaryPreferred}),
  case select_server(Topology, primary, Tags) of
    undefined -> select_server(Topology, secondary, Tags);
    Primary -> Primary
  end;
select_server(Topology, secondaryPreferred, Tags) ->
    erlang:display({secondaryPreferred}),
  case select_server(Topology, secondary, Tags) of
    undefined -> select_server(Topology, primary, Tags);
    Primary -> Primary
  end;
select_server(Topology, nearest, Tags) ->
    erlang:display({nearest}),
  get_nearest(select_server(Topology, primary, Tags), select_server(Topology, secondary, Tags));
select_server(Topology, Mode, Tags) ->
    erlang:display({something_lse, Mode}),
  {Tab, TType, Threshold} = mc_topology:get_state_part(Topology),
  LowestRTT = ets:foldl(fun count_lowest_rtt/2, 0, Tab),
  MaxRTT = LowestRTT + Threshold,
  Candidates = ets:foldl(
    fun(E, Acc) ->
      case is_candidate(Mode, Tags, E, MaxRTT) of
        false -> Acc;
        Res -> [Res | Acc]
      end
    end,
    [], Tab),
  erlang:display({candidates,Candidates, Tab, ets:tab2list(Tab)}),
  select_candidate(Mode, TType, Candidates).



% {candidates,[],#Ref<0.625201274.2658271233.73992>,[{mc_server,<0.223.0>,#Ref<0.625201274.2658140161.74049>,<<"localhost:27017">>,unknown,undefined,1144,1144,undefined,undefined,[],0,13,[],[],[],undefined,undefined,true,undefined}]}



% [{mc_server,<0.261.0>,#Ref<0.4232639252.779091969.61110>,<<"localhost:27017">>,standalone,undefined,1004,1004,undefined,undefined,[],0,13,[],[],[],undefined,undefined,true,undefined}]




%% @private
count_lowest_rtt(#mc_server{rtt = RTT, type = Type}, 0) when ?SUITABLE_TYPE(Type) -> RTT;
count_lowest_rtt(#mc_server{rtt = RTT, type = Type}, Acc) when ?SUITABLE_TYPE(Type) andalso RTT < Acc -> RTT;
count_lowest_rtt(_, Acc) -> Acc.

%% @private
get_nearest(undefined, A) -> A;
get_nearest(A, undefined) -> A;
get_nearest(#mc_server{rtt = RTT1} = A, #mc_server{rtt = RTT2}) when RTT1 < RTT2 -> A;
get_nearest(#mc_server{}, B = #mc_server{}) -> B.

%% @private
select_candidate(_, _, []) ->
  undefined;
select_candidate(primary, sharded, List) ->
  Len = length(List),
  pick_random(List, Len);
select_candidate(primary, _, PrimaryList) ->
  hd(PrimaryList);
select_candidate(secondary, _, List) ->
  Len = length(List),
  pick_random(List, Len).

%% @private
pick_random([Item], 1) -> Item;
pick_random(List, N) -> lists:nth(rand:uniform(N), List).

%% @private
is_candidate(_, _, #mc_server{type = Type}, _) when Type =/= rsPrimary, Type =/= rsSecondary, Type =/= mongos, Type =/= standalone ->
  false;
is_candidate(_, _, #mc_server{type = standalone} = Server, _) ->
  Server;
is_candidate(primary, _, #mc_server{type = rsPrimary} = Server, _) ->
  Server;
is_candidate(primary, _, #mc_server{type = mongos, rtt = RTT} = Server, MaxRTT) when RTT =< MaxRTT ->
  Server;
is_candidate(primary, _, #mc_server{type = _}, _) ->
  false;
is_candidate(secondary, Tags, #mc_server{type = rsSecondary, tags = STags, rtt = RTT} = Server, MaxRTT) when RTT =< MaxRTT ->
  check_tags(Server, Tags, STags);
is_candidate(secondary, _, #mc_server{type = mongos, rtt = RTT} = Server, MaxRTT) when RTT =< MaxRTT ->
  Server;
is_candidate(_, _, _, _) ->
  false.

%% @private
check_tags(Server, Tags, STags) ->
  ResL = length(lists:subtract(STags, lists:subtract(STags, Tags))),
  L = length(Tags),
  if
    ResL =/= L ->
      false;
    true ->
      Server
  end.

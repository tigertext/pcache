-module(pcache_tests).
-include_lib("eunit/include/eunit.hrl").

-export([tester/1, memoize_tester/1]).

-define(E(A, B), ?assertEqual(A, B)).
-define(_E(A, B), ?_assertEqual(A, B)).

pcache_setup() ->
  % start cache server tc (test cache)
  % 6 MB cache
  % 5 minute TTL per entry (300 seconds)
  {ok, Pid} = pcache_server:start_link(tc, pcache_tests, tester, 6, 300000),
  Pid.

pcache_cleanup(Cache) ->
    unregister(tc),
    exit(Cache, normal).

tester(Key) when is_binary(Key) orelse is_list(Key) ->
  erlang:md5(Key).

memoize_tester(Key) when is_binary(Key) orelse is_list(Key) ->
  erlang:crc32(Key).

pcache_test_() ->
  {setup,
    fun pcache_setup/0,
    fun pcache_cleanup/1,
    fun(_C) ->
      [
        ?_E(erlang:md5("bob"),  pcache:get(tc, "bob")),
        ?_E(erlang:md5("bob2"), pcache:get(tc, "bob2")),
        ?_E(ok,   pcache:dirty(tc, "bob2")),
        ?_E(ok,   pcache:dirty(tc, "bob2")),
        ?_E(erlang:crc32("bob2"),
            pcache:memoize(tc, ?MODULE, memoize_tester, "bob2")),
        ?_E(ok, pcache:dirty_memoize(tc, ?MODULE, memoize_tester, "bob2")),
        ?_E(0, pcache:total_size(tc)),
        ?_E([{cache_name, tc}, {datum_count, 1}], pcache:stats(tc)),
        ?_E(ok, pcache:empty(tc)),
        ?_E(0, pcache:total_size(tc))
      ]
    end
  }.
  
pcache_queue_test_() ->
    {setup, fun pcache_setup/0, fun pcache_cleanup/1,
     {with, [fun check_msg_queue_speed/1]}
     }.

load_msg_queue(Cache, Key, Num_Requesters, Caller) ->
    Notify_Fn = fun() -> Caller ! {datum, pcache:get(Cache, Key)} end,
    [spawn(Notify_Fn) || _N <- lists:seq(1,Num_Requesters)].
    
check_msg_queue_speed(Cache) ->
    Result = erlang:md5("jim"),
    ?assertMatch(Result, pcache:get(tc, "jim")),

    Msg_Count_1 = 1000,
    load_msg_queue(Cache, "jim", Msg_Count_1, self()),
    {Micros_1, ok} = timer:tc(fun() -> get_results(Msg_Count_1) end),
    Avg_Time_1 = Micros_1 / Msg_Count_1,

    Msg_Count_2 = 10000,
    load_msg_queue(Cache, "jim", Msg_Count_2, self()),
    {Micros_2, ok} = timer:tc(fun() -> get_results(Msg_Count_2) end),
    Avg_Time_2 = Micros_2 / Msg_Count_2,

    Msg_Count_3 = 40000,
    load_msg_queue(Cache, "jim", Msg_Count_3, self()),
    {Micros_3, ok} = timer:tc(fun() -> get_results(Msg_Count_3) end),
    Avg_Time_3 = Micros_3 / Msg_Count_3,

    Speeds = [[{msg_count, Msg_Count_1}, {avg_time, Avg_Time_1}, {fast_enough, 70 > Avg_Time_1}],
              [{msg_count, Msg_Count_2}, {avg_time, Avg_Time_2}, {fast_enough, 70 > Avg_Time_2}],
              [{msg_count, Msg_Count_3}, {avg_time, Avg_Time_3}, {fast_enough, 70 > Avg_Time_3}]],

    ?assertMatch([[{msg_count, Msg_Count_1}, {avg_time, Avg_Time_1}, {fast_enough, true}],
                  [{msg_count, Msg_Count_2}, {avg_time, Avg_Time_2}, {fast_enough, true}],
                  [{msg_count, Msg_Count_3}, {avg_time, Avg_Time_3}, {fast_enough, true}]],
                 Speeds).
                                     

get_results(0)     -> ok;
get_results(Count) ->
    receive {datum, _Result} -> get_results(Count-1)
    after 3000 -> timeout
    end.                         
                                     

-module(pcache_tests).
-include_lib("eunit/include/eunit.hrl").

-export([tester/1, memoize_tester/1, slow_tester/1, lookup_tester/1, crash_tester/1
        ]).

%% Spawned functions
-export([notify/3, many_same_gets/4, many_pings/4, many_same_fetches/5]).


%%% =======================================================================
%%% Test basic get/dirty, cache_size and crash tolerance
%%% =======================================================================

tester(Key) when is_binary(Key) orelse is_list(Key) -> erlang:md5(Key).
memoize_tester(Key) when is_binary(Key) orelse is_list(Key) -> erlang:crc32(Key).

pcache_setup() ->
  %% start cache server tc (test cache), 6 MB cache, 5 minute TTL per entry (300 seconds)
  {ok, Pid} = pcache_server:start_link(tc, ?MODULE, tester, 6, 300000),
  Pid.

pcache_cleanup(Cache) ->
    try
        pcache:empty(Cache),
        whereis(tc_pcache)  =:= undefined
            orelse begin unlink(whereis(tc_pcache)),  unregister(tc_pcache)  end,
        whereis(tc2_pcache) =:= undefined
            orelse begin unlink(whereis(tc2_pcache)), unregister(tc2_pcache) end
    catch Class:Error -> error_logger:error_msg("Caught ~p:~p~n", [Class, Error])
    end,
    exit(Cache, kill).  %% Fails with 'normal' for some reason.

pcache_test_() ->
  {foreach, fun pcache_setup/0, fun pcache_cleanup/1,
   [
    {with, [fun check_get_and_dirty/1]},
    {with, [fun check_cache_size/1]}
   ]}.

check_get_and_dirty(_Cache) ->
    Bob_Value = erlang:md5("bob"),
    Bob2_Value = erlang:md5("bob2"),
    ?assertMatch(Bob_Value,  pcache:get(tc_pcache, "bob")),
    ?assertMatch(Bob2_Value, pcache:get(tc_pcache, "bob2")),
    timer:sleep(10),
    Stats1 = pcache:stats(tc_pcache),
    ?assertMatch([tc, 2], [proplists:get_value(P, Stats1) || P <- [cache_name, datum_count]]),

    ?assertMatch(ok, pcache:dirty(tc_pcache, "bob2")),
    ?assertMatch(ok, pcache:dirty(tc_pcache, "bob2")),
    Bob2_Crc = erlang:crc32("bob2"),
    ?assertMatch(Bob2_Crc, pcache:memoize(tc_pcache, ?MODULE, memoize_tester, "bob2")),
    ?assertMatch(ok, pcache:dirty_memoize(tc_pcache, ?MODULE, memoize_tester, "bob2")),
    timer:sleep(10),

    Stats2 = pcache:stats(tc_pcache),
    ?assertMatch([tc, 1], [proplists:get_value(P, Stats2) || P <- [cache_name, datum_count]]),
    ?assertMatch(1, pcache:empty(tc_pcache)).

check_cache_size(Cache) ->
    ?assertMatch(0, pcache:total_size(Cache)),
    pcache:get(Cache, "bob"),
    timer:sleep(10),
    Size1 = pcache:total_size(Cache),
    ?assert(is_integer(Size1) andalso Size1 > 0),
    ?assertMatch(Size1, proplists:get_value(memory_used, pcache:stats(Cache))),
    pcache:get(tc_pcache, "bob2"),
    timer:sleep(10),
    Size2 = pcache:total_size(Cache),
    ?assert(is_integer(Size2) andalso Size2 > Size1),
    ?assertMatch(Size2, proplists:get_value(memory_used, pcache:stats(Cache))),
    pcache:dirty(Cache, "bob2"),
    timer:sleep(10),
    ?assertMatch(Size1, pcache:total_size(Cache)),
    ?assertMatch(Size1, proplists:get_value(memory_used, pcache:stats(Cache))),

    Bob2 = pcache:get(Cache, "bob2"),
    timer:sleep(10),
    ?assertMatch(Size2, pcache:total_size(Cache)),
    ?assertMatch(Size2, proplists:get_value(memory_used, pcache:stats(Cache))),
    ?assertMatch(Bob2, erlang:md5("bob2")),
    Long_Value = lists:duplicate(3,"supercalifragilisticexpialidocious"),
    pcache:dirty(Cache, "bob2", Long_Value),
    timer:sleep(10),
    ?assertMatch(Long_Value, pcache:get(Cache, "bob2")),
    Size3 = pcache:total_size(Cache),
    ?assert(is_integer(Size3) andalso Size3 > Size2),
    ?assertMatch(Size3, proplists:get_value(memory_used, pcache:stats(Cache))),

    %% Empty takes immediate effect...
    ?assertMatch(2, pcache:empty(Cache)),
    ?assertMatch(0, pcache:total_size(Cache)),
    ?assertMatch(0, proplists:get_value(memory_used, pcache:stats(Cache))),

    %% And late messages don't make size negative.
    pcache:get(Cache, "bob"),
    timer:sleep(10),
    Size1 = pcache:total_size(Cache),
    ?assert(is_integer(Size1) andalso Size1 > 0),
    ?assertMatch(Size1, proplists:get_value(memory_used, pcache:stats(Cache))),
    pcache:get(tc_pcache, "bob2"),
    timer:sleep(10),
    Size2 = pcache:total_size(Cache),
    ?assert(is_integer(Size2) andalso Size2 > Size1),
    ?assertMatch(Size2, proplists:get_value(memory_used, pcache:stats(Cache))),
    pcache:dirty(Cache, "bob2"),
    timer:sleep(10),
    ?assertMatch(Size1, pcache:total_size(Cache)),
    ?assertMatch(Size1, proplists:get_value(memory_used, pcache:stats(Cache))),

    ?assertMatch(1, pcache:empty(Cache)),
    ?assertMatch(0, pcache:total_size(Cache)),
    ?assertMatch(0, proplists:get_value(memory_used, pcache:stats(Cache))).

crash_tester(Key) -> Key ++ " broke me".

pcache_datum_crash_setup() ->
  {ok, Pid} = pcache_server:start_link(tc, ?MODULE, crash_tester, 6, 300000),
  Pid.

pcache_datum_crash_test_() ->
  {setup, fun pcache_datum_crash_setup/0, fun pcache_cleanup/1,
   {with, [fun check_mfa_crash/1]}
  }.

check_mfa_crash(Cache) ->
%%    ?assertMatch('** pcache_tests:crash_tester(<<"Binary_Token">>) Crashed! error:badarg **',
    ?assertException(exit,{timeout,_}, pcache:get(Cache, <<"Binary_Token">>)),
    ok.
    
  

%%% =======================================================================
%%% Test the speed of gets when the gen_server message queue is full
%%% =======================================================================
pcache_queue_test_() ->
    {setup, fun pcache_setup/0, fun pcache_cleanup/1,
     {with, [fun check_msg_queue_speed/1]}
    }.

load_msg_queue(Cache, Key, Num_Requesters, Caller) ->
    Notify_Fn = fun() -> Caller ! {datum, pcache:get(Cache, Key)} end,
    load_1_msg(Notify_Fn, Num_Requesters).
    
load_1_msg(_Notify_Fn, 0) -> ok;
load_1_msg(Notify_Fn, N) -> spawn(Notify_Fn), load_1_msg(Notify_Fn, N-1).
                             
    
check_msg_queue_speed(Cache) ->
    Result = erlang:md5("jim"),
    ?assertMatch(Result, pcache:get(tc_pcache, "jim")),

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

    %% 70 microseconds on avg to fetch values when queue is full...
    Speeds = [[{msg_count, Msg_Count_1}, {avg_time, Avg_Time_1}, {fast_enough, 70 > Avg_Time_1}],
              [{msg_count, Msg_Count_2}, {avg_time, Avg_Time_2}, {fast_enough, 70 > Avg_Time_2}],
              [{msg_count, Msg_Count_3}, {avg_time, Avg_Time_3}, {fast_enough, 70 > Avg_Time_3}]],

    ?assertMatch([[{msg_count, Msg_Count_1}, {avg_time, Avg_Time_1}, {fast_enough, true}],
                  [{msg_count, Msg_Count_2}, {avg_time, Avg_Time_2}, {fast_enough, true}],
                  [{msg_count, Msg_Count_3}, {avg_time, Avg_Time_3}, {fast_enough, true}]],
                 Speeds),
    ok.

get_results(0)     -> ok;
get_results(Count) ->
    receive {datum, _Result} -> get_results(Count-1)
    after 3000 -> timeout
    end.                         


%%% =======================================================================
%%% Test that slow new value M:F(A) doesn't stall get requests
%%% =======================================================================
pcache_slow_setup() ->
  {ok, Pid} = pcache_server:start_link(tc, ?MODULE, slow_tester, 6, 300000),
  Pid.

-define(SLOW, 700).

slow_tester(Key) when is_binary(Key) orelse is_list(Key) ->
    timer:sleep(?SLOW),
    erlang:md5(Key).
                                     
pcache_spawn_test_() ->
    {setup, fun pcache_slow_setup/0, fun pcache_cleanup/1,
     {with, [fun check_spawn_speed/1]}
     }.

notify(Caller, Cache, Existing_Key) ->
    {Micros, Result} = timer:tc(pcache, get, [Cache, Existing_Key]),
    Caller ! {datum, Existing_Key, Micros, Result}.
    
fetch_timing(Cache, Existing_Key, New_Key) ->
    Caller = self(),
    %% Attempt to plug up the server generating a new key...
    spawn(pcache, get, [Cache, New_Key]),
    %% While waiting for existing key fetches.
    [spawn(?MODULE, notify, [Caller, Cache, Existing_Key]) || _N <- lists:seq(1,5)],

    get_key_results(5, []).

get_key_results(0,     Results) -> Results;
get_key_results(Count, Results) ->
    receive Datum -> get_key_results(Count-1, [Datum | Results])
    after (?SLOW*4) -> timeout
    end.                         
    
check_spawn_speed(Cache) ->
    Existing_Result = erlang:md5("existing_key"),
    {Micros_Existing, Get_Existing_New} = timer:tc(fun() -> pcache:get(Cache, "existing_key", 2000) end),
    ?assertMatch(Existing_Result, Get_Existing_New),
    ?assert((?SLOW * 1000) < Micros_Existing),

    %% 300 microseconds to fetch an existing value queued behind
    %% a new value construction that takes 1000 microseconds
    Results = fetch_timing(Cache, "existing_key", "created_key"),
    ?assertMatch(5, length(Results)),
    Slow_Fetches = [[{latency, Micros}, {key, Key}, {result, Result}]
                    || {datum, Key, Micros, Result} <- Results, Micros > 300],
    ?assertMatch([], Slow_Fetches),
    ?assertMatch([], [R || {datum, _Key, _Micros, R} <- Results, R =/= Existing_Result]),
    ok.

%%% =======================================================================
%%% Test dirty followed immediately by get doesn't timeout
%%% =======================================================================
                                     
pcache_timeout_test_() ->
    {setup, fun pcache_setup/0, fun pcache_cleanup/1,
     {with, [fun check_dirty_timeout/1]}
     }.

check_dirty_timeout(Cache) ->
    Bob_Value = erlang:md5("bob"),
    Bob2_Value = erlang:md5("bob2"),
    ?assertMatch(Bob_Value,  pcache:get(Cache, "bob")),
    ?assertMatch(Bob2_Value, pcache:get(Cache, "bob2")),
    timer:sleep(10),
    Stats = pcache:stats(tc_pcache),
    ?assertMatch([tc, 2], [proplists:get_value(P, Stats) || P <- [cache_name, datum_count]]),

    ?assertMatch(ok, pcache:dirty(Cache, "bob2")),
    ?assertMatch(Bob2_Value, pcache:get(Cache, "bob2")),
    ?assertMatch(Bob_Value, pcache:get(Cache, "bob")),
    ok.


%%% =======================================================================
%%% Test that TTL and reaper culls the oldest values
%%% =======================================================================
pcache_fast_ttl_setup() ->
  %% start cache server tc (test cache), 6 MB cache, 2 second TTL per entry
  {ok, Pid} = pcache_server:start_link(tc, ?MODULE, tester, 6, 2000),
  Pid.

pcache_ttl_test_() ->
  {setup, fun pcache_fast_ttl_setup/0, fun pcache_cleanup/1,
    {with, [fun check_default_ttl/1, fun check_ttl/1]}
  }.

check_default_ttl(Cache) ->
    ?assertMatch(2000, proplists:get_value(default_ttl, pcache:stats(Cache))),
    ?assertMatch(2000, pcache:change_default_ttl(Cache, 3000)),
    ?assertMatch(3000, proplists:get_value(default_ttl, pcache:stats(Cache))),
    ?assertMatch(3000, pcache:change_default_ttl(Cache, 2000)),
    ok.

check_ttl(Cache) ->
    pcache:get(Cache, "jim1"),
    timer:sleep(1000),
    pcache:get(Cache, "jim2"),
    timer:sleep(100),
    ?assertMatch(2, proplists:get_value(datum_count, pcache:stats(Cache))),
    timer:sleep(1000),
    ?assertMatch(1, proplists:get_value(datum_count, pcache:stats(Cache))),
    timer:sleep(1000),
    ?assertMatch(0, proplists:get_value(datum_count, pcache:stats(Cache))),
    ok.

pcache_oldest_test_() ->
  {setup, fun pcache_setup/0, fun pcache_cleanup/1,
    {with, [fun check_reap_oldest/1]}
  }.

check_reap_oldest(Cache) ->
    pcache:get(Cache, "jim1"),
    timer:sleep(100),
    pcache:get(Cache, "jim2"),
    timer:sleep(100),
    pcache:get(Cache, "jim3"),
    timer:sleep(100),
    ?assertMatch(3, proplists:get_value(datum_count, pcache:stats(Cache))),
    Ages1 = lists:sort(gen_server:call(Cache, ages)),
    gen_server:call(Cache, reap_oldest),
    timer:sleep(1000),
    Ages2 = lists:sort(gen_server:call(Cache, ages)),
    ?assertMatch(Ages2, tl(Ages1)),
    ok.

pcache_lemmings_test_() ->
  {setup, fun pcache_fast_ttl_setup/0, fun pcache_cleanup/1,
    {with, [fun check_mass_expire/1]}
  }.

make_jims(Cache, Start, End) ->
    [pcache:get(Cache, Key)
     || Key <- ["jim" ++ integer_to_list(N) || N <- lists:seq(Start, End)]].
    
check_mass_expire(Cache) ->
    make_jims(Cache, 1, 20),
    timer:sleep(800),
    make_jims(Cache, 21, 40),
    timer:sleep(400),
    make_jims(Cache, 41, 60),
    timer:sleep(500),
    ?assertMatch(60, proplists:get_value(datum_count, pcache:stats(Cache))),
    pcache:expire(Cache, 20),   %% Expire 1st wave of jims
    timer:sleep(100),
    ?assertMatch(40, proplists:get_value(datum_count, pcache:stats(Cache))),
    ok.

%%% =======================================================================
%%% Test random values
%%% =======================================================================
    
lookup_tester("fred1") -> 1;
lookup_tester("fred2") -> 2;
lookup_tester("fred3") -> 3;
lookup_tester("fred4") -> 4;
lookup_tester("fred5") -> 5;
lookup_tester("fred6") -> 6.

pcache_lookup_setup() ->
  %% start cache server tc (test cache), 6 MB cache, 5 minute TTL per entry (300 seconds)
  {ok, Pid} = pcache_server:start_link(tc, ?MODULE, lookup_tester, 6, 300000),
  Pid.

pcache_random_test_() ->
  {setup, fun pcache_lookup_setup/0, fun pcache_cleanup/1,
    {with, [fun check_rand/1]}
  }.

check_rand(Cache) ->
    pcache:get(Cache, "fred1"),
    pcache:get(Cache, "fred2"),
    pcache:get(Cache, "fred3"),
    pcache:get(Cache, "fred4"),
    pcache:get(Cache, "fred5"),
    pcache:get(Cache, "fred6"),
    timer:sleep(100),
    ?assertMatch(6, proplists:get_value(datum_count, pcache:stats(Cache))),

    Rand_Val_1 = [Value || {ok, Value} <- pcache:rand(Cache, 3)],
    Rand_Val_2 = [Value || {ok, Value} <- pcache:rand(Cache, 3)],
    ?assertMatch(3, length([V || V <- Rand_Val_1, is_integer(V), V > 0, V < 7])),
    ?assertMatch(3, length(Rand_Val_1)),
    ?assertMatch(3, length([V || V <- Rand_Val_2, is_integer(V), V > 0, V < 7])),
    ?assertMatch(3, length(Rand_Val_2)),
    ?assert(Rand_Val_1 =/= Rand_Val_2),

    Rand_Key_1 = [Key || {ok, Key} <- pcache:rand_keys(Cache, 3)],
    Rand_Key_2 = [Key || {ok, Key} <- pcache:rand_keys(Cache, 3)],
    ?assertMatch(3, length([K || K <- Rand_Key_1, string:substr(K, 1, 4) == "fred"])),
    ?assertMatch(3, length(Rand_Key_1)),
    ?assertMatch(3, length([K || K <- Rand_Key_2, string:substr(K, 1, 4) == "fred"])),
    ?assertMatch(3, length(Rand_Key_2)),
    ?assert(Rand_Key_1 =/= Rand_Key_2),
    ok.

%%% =======================================================================
%%% Test get vs. last_active performance (cost of now())
%%% =======================================================================

pcache_speed_test_() ->
  {setup, fun pcache_fast_ttl_setup/0, fun pcache_cleanup/1,
    {with, [fun check_ets_speed/1, fun check_pdict_speed/1]}
  }.

check_ets_speed(Cache) ->
    Key = "jim1",
    V1 = pcache:get(Cache, Key),
    timer:sleep(100),
    V2 = pcache:age(Cache, Key),
    Repeat_Count = 100000,
    Millis_Per_Micro = 1000000,

    Key_Fetch = "jimFetch",
    V_Fetch = pcache:fetch(Cache, tc, Key_Fetch),

    %% Try using get (which funnels through a central gen_server)...
    {Time1, _Result1} = timer:tc(?MODULE, many_same_gets, [Cache, Key, V1, Repeat_Count]),
    Seconds1 = Time1 / Millis_Per_Micro,
    error_logger:info_msg("~p pcache:get requests take ~p seconds at ~p reqs/sec~n",
                          [Repeat_Count, Seconds1, Repeat_Count / Seconds1 ]),

    %% Try using fetch (which uses read_concurrency on an ets table)...
    {Time2, _Result2} = timer:tc(?MODULE, many_same_fetches, [Cache, tc, Key_Fetch, V_Fetch, Repeat_Count]),
    Seconds2 = Time2 / Millis_Per_Micro,
    error_logger:info_msg("~p pcache:fetch requests take ~p seconds at ~p reqs/sec~n",
                           [Repeat_Count, Seconds2, Repeat_Count / Seconds2 ]),

    %% Check the age of an item repeatedly.
    {Time3, _Result3} = timer:tc(?MODULE, many_pings, [Cache, Key, V2, Repeat_Count]),
    Seconds3 = Time3 / Millis_Per_Micro,
    error_logger:info_msg("~p pcache:age requests take ~p seconds at ~p reqs/sec~n",
                          [Repeat_Count, Seconds3, Repeat_Count / Seconds3]),
    ok.

check_pdict_speed(Cache) ->
    Key = "jim2",
    V1 = pcache:get(Cache, Key),
    timer:sleep(100),
    V2 = pcache:age(Cache, Key),
    Repeat_Count = 100000,
    Millis_Per_Micro = 1000000,

    {Time1, _Result1} = timer:tc(?MODULE, many_same_gets, [Cache, Key, V1, Repeat_Count]),
    Seconds1 = Time1 / Millis_Per_Micro,
    error_logger:info_msg("~p pcache:get pdict requests take ~p seconds at ~p reqs/sec~n",
                          [Repeat_Count, Seconds1, Repeat_Count / Seconds1]),
    {Time2, _Result2} = timer:tc(?MODULE, many_pings, [Cache, Key, V2, Repeat_Count]),
    Seconds2 = Time2 / Millis_Per_Micro,
    error_logger:info_msg("~p pcache:age pdict requests take ~p seconds at ~p reqs/sec~n",
                          [Repeat_Count, Seconds2, Repeat_Count / Seconds2]),
    ok.

many_same_gets(_Cache, _Key, _Val, 0) -> ok;
many_same_gets(Cache, Key, Val, N) ->
    Val = pcache:get(Cache, Key),
    many_same_gets(Cache, Key, Val, N-1).

many_same_fetches(_Server, _Ets, _Key, _Val, 0) -> ok;
many_same_fetches(Server, Ets, Key, Val, N) ->
    Val = pcache:fetch(Server, Ets, Key),
    many_same_fetches(Server, Ets, Key, Val, N-1).
    
many_pings(_Cache, _Key, _Val, 0) -> ok;
many_pings(Cache, Key, _Val, N) ->
    pcache:age(Cache, Key),
    many_pings(Cache, Key, _Val, N-1).
    

-module(pcache_reaper).

-behaviour(gen_server).

-export([start/2]).
-export([start_link/1, start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([pcache_reaper/2]). % quiet unused function annoyance
-record(reaper, {cache_size}).

start_link(Name) ->
  start_link(Name, 8).

start_link(CacheName, CacheSize) ->
  gen_server:start_link(?MODULE, [CacheName, CacheSize], []).

start(CacheName, CacheSize) ->
  gen_server:start(?MODULE, [CacheName, CacheSize], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

-define(DEFAULT_PCT_TTL,           20).
-define(DEFAULT_CHECK_FREQUENCY, 4000).

pcache_reaper(Name, Cache_Size) ->
    pcache_reaper(Name, Cache_Size, ?DEFAULT_CHECK_FREQUENCY).

pcache_reaper(Name, Cache_Size, Check_Frequency) ->
    case receive after Check_Frequency -> pcache:total_size(Name) end of
        Current_Size when Current_Size < Cache_Size -> ok;
        _Current_Size ->
            %% io:format("Cache ~p too big!  Shrinking...~n", [self()]),
            %% io:format("CurrentSize: ~p; Target Size: ~p~n", [_Current_Size, Cache_Size]),
            pcache:expire(Name, ?DEFAULT_PCT_TTL)
    end,
    pcache_reaper(Name, Cache_Size, Check_Frequency).
    
init([Name, CacheSizeBytes]) ->
  % pcache_reaper is started from pcache_server, but pcache_server can't finish
  % init'ing % until pcache_reaper:init/1 returns.
  % Use apply_after to make sure pcache_server exists when making calls.
  % Don't be clever and take this timer away.  Compensates for chicken/egg prob.
  timer:apply_after(?DEFAULT_CHECK_FREQUENCY, ?MODULE, pcache_reaper, [Name, CacheSizeBytes]),
  State = #reaper{cache_size = CacheSizeBytes},
  {ok, State}.

handle_call(Arbitrary, _From, State) ->
  {reply, {arbitrary, Arbitrary}, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(_Info, State) ->
  %% io:format("Other info of: ~p~n", [Info]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

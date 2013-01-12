-module(pcache_server).

-behaviour(gen_server).

-export([start_link/3, start_link/4, start_link/5, start_link/6]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

%% Spawned functions
-export([datum_launch/7, datum_loop/1]).

-record(cache, {name, datum_index, data_module, 
                reaper_pid, data_accessor, cache_size,
                cache_policy, default_ttl, cache_used = 0}).

% make 8 MB cache
start_link(Name, Mod, Fun) ->
  start_link(Name, Mod, Fun, 8).

% make 5 minute expiry cache
start_link(Name, Mod, Fun, CacheSize) ->
  start_link(Name, Mod, Fun, CacheSize, 300000).

% make MRU policy cache
start_link(Name, Mod, Fun, CacheSize, CacheTime) ->
  start_link(Name, Mod, Fun, CacheSize, CacheTime, mru).

start_link(Name, Mod, Fun, CacheSize, CacheTime, CachePolicy) ->
  gen_server:start_link({local, Name}, 
    ?MODULE, [Name, Mod, Fun, CacheSize, CacheTime, CachePolicy], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

init([Name, Mod, Fun, CacheSize, CacheTime, CachePolicy]) ->
  DatumIndex = ets:new(Name, [set, private]),
  CacheSizeBytes = CacheSize*1024*1024,

  {ok, ReaperPid} = pcache_reaper:start(Name, CacheSizeBytes),
  erlang:monitor(process, ReaperPid),

  State = #cache{name = Name,
                 datum_index = DatumIndex,
                 data_module = Mod,
                 data_accessor = Fun,
                 reaper_pid = ReaperPid,
                 default_ttl = CacheTime,
                 cache_policy = CachePolicy,
                 cache_size = CacheSizeBytes,
                 cache_used = 0},
  {ok, State}.

locate(DatumKey, #cache{datum_index = DatumIndex, data_module = DataModule,
                  default_ttl = DefaultTTL, cache_policy = Policy, data_accessor = DataAccessor}) ->
  UseKey = key(DatumKey),
  case ets:lookup(DatumIndex, UseKey) of 
    [{UseKey, Pid, _Size}] when is_pid(Pid) -> Pid;
    [] -> launch_datum(DatumKey, UseKey, DatumIndex, DataModule, DataAccessor, DefaultTTL, Policy)
  end.

locate_memoize(DatumKey, DatumIndex, DataModule, DataAccessor, DefaultTTL, Policy) ->
  UseKey = key(DataModule, DataAccessor, DatumKey),
  case ets:lookup(DatumIndex, UseKey) of 
    [{UseKey, Pid, _Size}] when is_pid(Pid) -> Pid;
    [] -> launch_memoize_datum(DatumKey, UseKey, DatumIndex, DataModule, DataAccessor, DefaultTTL, Policy)
  end.

handle_call({location, DatumKey}, _From, State) -> 
  {reply, locate(DatumKey, State), State};

handle_call({generic_get, M, F, Key}, From,
            #cache{datum_index = DatumIndex, default_ttl = DefaultTTL, cache_policy = Policy} = State) ->
    %% io:format("Requesting: ~p:~p(~p)~n", [M, F, Key]),
    DatumPid = locate_memoize(Key, DatumIndex, M, F, DefaultTTL, Policy),
    DatumPid ! {get, From},
    {noreply, State};

handle_call({get, Key}, From, #cache{} = State) ->
    %% io:format("Requesting: (~p)~n", [Key]),
    DatumPid = locate(Key, State),
    DatumPid ! {get, From},
    {noreply, State};

handle_call(total_size, _From, #cache{cache_used = Used} = State) ->
  {reply, Used, State};

handle_call(stats, _From, #cache{datum_index = DatumIndex} = State) ->
  EtsInfo = ets:info(DatumIndex),
  CacheName = proplists:get_value(name, EtsInfo),
  DatumCount = proplists:get_value(size, EtsInfo),
  Stats = [{cache_name, CacheName},
           {datum_count, DatumCount}],
  {reply, Stats, State};

handle_call(empty, _From, #cache{datum_index = DatumIndex} = State) ->
    %% We don't wait synchronously, so the make_ref() is just for a consistent interface message.
    Ref = make_ref(),
    Num_Destroyed = ets:foldl(fun({_DatumKey, DatumPid, _Size}, Num) ->
                                      DatumPid ! {destroy, Ref, self()}, Num+1
                              end, 0, DatumIndex),
  {reply, Num_Destroyed, State};

handle_call(reap_oldest, _From, #cache{datum_index = DatumIndex} = State) ->
  GetOldest = fun({_UseKey, DatumPid, _Size}, {APid, Acc}) ->
                Ref = make_ref(),
                DatumPid ! {last_active, Ref, self()},
                receive
                  {last_active, Ref, DatumPid, LastActive} ->
                    if
                      Acc =< LastActive -> {APid, Acc};
                      true -> {DatumPid, LastActive}
                    end
                 after 200 -> {APid, Acc}
                 end
               end,
  {OldPid, _LActive} = ets:foldl(GetOldest, {false, {9999,0,0}}, DatumIndex),
  case OldPid of
    false -> no_datum;
    _ -> Ref = make_ref(),
         OldPid ! {destroy, Ref, self()}
  end,
  {reply, ok, State};

handle_call({rand, Type, Count}, _From, 
  #cache{datum_index = DatumIndex} = State) ->
  AllPids = case ets:match(DatumIndex, {'_', '$1', '_'}) of
              [] -> [];
              Found -> lists:flatten(Found)
            end, 
  Length = length(AllPids),
  FoundData = 
  case Length =< Count of
    true  -> case Type of
               data -> [get_data(P) || P <- AllPids];
               keys -> [get_key(P) || P <- AllPids]
             end;
    false ->  RandomSet  = [crypto:rand_uniform(1, Length) || 
                              _ <- lists:seq(1, Count)],
              RandomPids = [lists:nth(Q, AllPids) || Q <- RandomSet],
              case Type of
                data -> [get_data(P) || P <- RandomPids];
                keys -> [get_key(P) || P <- RandomPids]
              end
  end,
  {reply, FoundData, State};
  
handle_call(Arbitrary, _From, State) ->
  {reply, {arbitrary, Arbitrary}, State}.

handle_cast({dirty, DatumKey, NewData}, #cache{datum_index = DatumIndex} = State) ->
    UseKey = key(DatumKey),
    case ets:lookup(DatumIndex, UseKey) of
        [{UseKey, DatumPid, _Size}] -> new_data(DatumPid, NewData);
        [] -> noop
    end,
    %% TODO: What about cache_size?
    {noreply, State};

handle_cast({dirty, DatumKey}, #cache{datum_index = DatumIndex, cache_used=Used} = State) ->
    UseKey = key(DatumKey),
    New_Size = case ets:lookup(DatumIndex, UseKey) of
    [{UseKey, DatumPid, Size}] -> Ref = make_ref(),
                         DatumPid ! {destroy, Ref, self()},
                   receive 
                     {destroy, Ref, DatumPid, ok} -> ets:delete(DatumIndex, UseKey),
                                                Used - Size
                   after 
                     100 -> Used 
                   end;
    [] -> Used 
  end,
  {noreply, State#cache{cache_used = New_Size}};

handle_cast({generic_dirty, M, F, A},
    #cache{datum_index = DatumIndex} = State) ->
    UseKey = key(M, F, A),
    case ets:lookup(DatumIndex, UseKey) of
        [{UseKey, DatumPid, _}] -> Ref = make_ref(),
                             DatumPid ! {destroy, Ref, self()},
                   receive 
                     {destroy, Ref, DatumPid, ok} -> ok
                   after 
                     100 -> fail
                   end;
    [] -> ok
  end,
    %% TODO: What about cache_size?
  {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% New datum is inserted at 0 size initially, then updated with real size by a message.
handle_info({new_datum_size, {UseKey, DatumPid, Size}},
            #cache{datum_index = DatumIndex, cache_used = Used} = State) ->
  ets:insert(DatumIndex, {UseKey, DatumPid, Size}),
  {noreply, State#cache{cache_used = Used + Size}};

handle_info({destroy, _Ref, _DatumPid, ok}, State) ->
  {noreply, State};

handle_info({'DOWN', _Ref, process, ReaperPid, _Reason}, 
    #cache{reaper_pid = ReaperPid, name = Name, cache_size = Size} = State) ->
  {NewReaperPid, _Mon} = pcache_reaper:start_link(Name, Size),
  {noreply, State#cache{reaper_pid = NewReaperPid}};

handle_info({'DOWN', _Ref, process, DatumPid, _Reason},
    #cache{datum_index = DatumIndex, cache_used=Used} = State) ->

  %% Remove the _one_ pending 'new_datum_size' message, if it exists...
  receive {new_datum_size, {_UseKey, DatumPid, _Size}} -> noop
  after 0 -> none
  end,

  New_State = case ets:match_object(DatumIndex, {'_', DatumPid, '_'}) of
                  [{Key, _, Size}] -> ets:delete(DatumIndex, Key),
                                      State#cache{cache_used = Used - Size};
                  [] -> State 
  end,
  {noreply, New_State};

handle_info(_Info, State) ->
  %% io:format("Other info of: ~p~n", [Info]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


key(Key) -> Key.
key(M, F, A) -> {M, F, A}.


%% ===================================================================
%% Private
%% ===================================================================

new_data(DatumPid, NewData) -> new_data(DatumPid, NewData, 100).
new_data(DatumPid, NewData, Timeout) ->
  Ref = make_ref(),
  DatumPid ! {new_data, Ref, self(), NewData},
  receive {new_data, Ref, DatumPid, _OldData} -> ok
  after Timeout -> fail
  end.

get_data(DatumPid) -> get_data(DatumPid, 100).
get_data(DatumPid, Timeout) ->
  Ref = make_ref(),
  DatumPid ! {get, Ref, self()},
  receive {get, Ref, DatumPid, Data} -> {ok, Data}
  after Timeout -> {no_data, timeout}
  end.

get_key(DatumPid) -> get_key(DatumPid, 100).
get_key(DatumPid, Timeout) ->
  Ref = make_ref(),
  DatumPid ! {getkey, Ref, self()},
  receive {getkey, Ref, DatumPid, Key} -> {ok, Key}
  after Timeout -> {no_data, timeout}
  end.

-record(datum, {key, mgr, data, started,
                last_active, ttl, type = mru, remaining_ttl}).

create_datum(Cache_Server, Key, Data, TTL, Type) ->
  #datum{key = Key, mgr = Cache_Server, data = Data, started = now(),
         ttl = TTL, remaining_ttl = TTL, type = Type}.

make_new_datum(Cache_Server, Key, UseKey, Module, Accessor, TTL, CachePolicy) ->
  CacheData = Module:Accessor(Key),
  create_datum(Cache_Server, UseKey, CacheData, TTL, CachePolicy).

launch_datum(DatumKey, UseKey, EtsIndex, Module, Accessor, TTL, CachePolicy) ->
  Datum_Args = [self(), DatumKey, UseKey, Module, Accessor, TTL, CachePolicy],
  {Datum_Pid, _Monitor} = erlang:spawn_monitor(?MODULE, datum_launch, Datum_Args),
  ets:insert(EtsIndex, {UseKey, Datum_Pid, 0}),
  Datum_Pid.

launch_memoize_datum(DatumKey, UseKey, EtsIndex, Module, Accessor, TTL, CachePolicy) ->
  Datum_Args = [self(), DatumKey, UseKey, Module, Accessor, TTL, CachePolicy],
  {Datum_Pid, _Monitor} = erlang:spawn_monitor(?MODULE, datum_launch, Datum_Args),
  ets:insert(EtsIndex, {UseKey, Datum_Pid, 0}),
  Datum_Pid.


%%% =======================================================================
%%% Datum launch and message response functions
%%% =======================================================================

datum_launch(Cache_Server, Key, UseKey, Module, Accessor, TTL, CachePolicy) ->
    Datum = make_new_datum(Cache_Server, Key, UseKey, Module, Accessor, TTL, CachePolicy),
    Datum_Pid = self(),
    {_, Size} = process_info(Datum_Pid, memory),
    Cache_Server ! {new_datum_size, {UseKey, Datum_Pid, Size}},
    datum_loop(Datum).

update_ttl(#datum{started = Started, ttl = TTL,
                  type = actual_time} = Datum) ->
  % Get total time in seconds this datum has been running.  Convert to ms.
  StartedNowDiff = (calendar:time_to_seconds(now()) - 
                    calendar:time_to_seconds(Started)) * 1000,
  % If we are less than the TTL, update with TTL-used (TTL in ms too)
  % else, we ran out of time.  expire on next loop.
  TTLRemaining = if
                   StartedNowDiff < TTL -> TTL - StartedNowDiff;
                                   true -> 0
                 end,
  Datum#datum{last_active = now(), remaining_ttl = TTLRemaining};
update_ttl(Datum) ->
  Datum#datum{last_active = now()}.

update_data(Datum, NewData) ->
  Datum#datum{data = NewData}.
    
continue(Datum) ->
  datum_loop(update_ttl(Datum)).

continue_noreset(Datum) ->
  datum_loop(Datum).

datum_loop(#datum{key = Key, mgr = Mgr, last_active = LastActive,
            data = Data, remaining_ttl = TTL} = State) ->
  receive
    {new_data, Ref, Mgr, Replacement} ->
      Mgr ! {new_data, Ref, self(), Data},
      continue(update_data(Replacement, State));

    {InvalidMgrRequest, Ref, Mgr, _Other} ->
      Mgr ! {InvalidMgrRequest, Ref, self(), invalid_creator_request},
      continue(State);

    {get, Gen_Server_From} ->
      gen_server:reply(Gen_Server_From, Data),
      continue(State);

    {getkey, Gen_Server_From} ->
      gen_server:reply(Gen_Server_From, Key),
      continue(State);

    {memsize, Ref, From} ->
      Size = case is_binary(Data) of
               true -> {memory, size(Data)};
                  _ -> process_info(self(), memory)
             end,
      From ! {memsize, Ref, self(), Size},
      continue_noreset(State);

    {last_active, Ref, From} ->
      From ! {last_active, Ref, self(), LastActive},
      continue(State);

    {destroy, Ref, From} ->
      From ! {destroy, Ref, self(), ok},
      % io:format("destroying ~p with last access of ~p~n", [self(), LastActive]),
      exit(self(), destroy);

    {InvalidRequest, Ref, From} ->
      From ! {InvalidRequest, Ref, self(), invalid_request},
      continue(State);

    _ -> continue(State)

  after
    TTL ->
      cache_is_now_dead
        % INSERT STATS COLLECTION INFO HERE
        % io:format("Cache object ~p owned by ~p freed~n", [Key, self()])
  end.

-module(pcache_server).

-behaviour(gen_server).

-export([start_link/3, start_link/4, start_link/5, start_link/6]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

%% Spawned functions
-export([datum_launch/7, datum_loop/1]).

-type cache_policy() :: mru | actual_time.

-record(cache, {
          name             :: atom(),
          datum_index      :: ets:tab(),
          data_module      :: module(), 
          reaper_pid       :: pid(),
          data_accessor    :: atom(),
          cache_size       :: pos_integer(),
          cache_policy     :: cache_policy(),
          default_ttl      :: pos_integer(),
          cache_used = 0   :: non_neg_integer()
         }).

% make 8 MB cache
start_link(Name, Mod, Fun) ->
  start_link(Name, Mod, Fun, 8).

% make 5 minute expiry cache
start_link(Name, Mod, Fun, CacheSize) ->
  start_link(Name, Mod, Fun, CacheSize, 300000).

% make MRU policy cache
start_link(Name, Mod, Fun, CacheSize, CacheTime) ->
  start_link(Name, Mod, Fun, CacheSize, CacheTime, mru).

start_link(Name, Mod, Fun, CacheSize, CacheTime, CachePolicy)
  when CachePolicy =:= mru; CachePolicy =:= actual_time ->
    Args = [Name, Mod, Fun, CacheSize, CacheTime, CachePolicy],
    gen_server:start_link({local, Name}, ?MODULE, Args, []).


%%%----------------------------------------------------------------------
%%% Init and gen_server state, terminate, code_change and locate functions
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

code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

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

key(Key) -> Key.
key(M, F, A) -> {M, F, A}.


%%%----------------------------------------------------------------------
%%% handle_call messages
%%%----------------------------------------------------------------------

%% Only to allow for testing of reap_oldest
handle_call(ages, _From, #cache{datum_index = DatumIndex} = State) ->
    All_Datums = ets:foldl(fun({_UseKey, DatumPid, _Size}, Pids) -> [DatumPid | Pids] end, [], DatumIndex),
    Ages = [get_age(Pid) || Pid <- All_Datums],
    {reply, Ages, State};

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
  Stats = [{cache_name, CacheName}, {datum_count, DatumCount}],
  {reply, Stats, State};

handle_call(empty, _From, #cache{datum_index = DatumIndex} = State) ->
    %% We don't wait synchronously, so the make_ref() is just for a consistent interface message.
    Ref = make_ref(),
    Num_Destroyed = ets:foldl(fun({_DatumKey, DatumPid, _Size}, Num) ->
                                      DatumPid ! {destroy, Ref, self()}, Num+1
                              end, 0, DatumIndex),
  {reply, Num_Destroyed, State};

handle_call(reap_oldest, _From, #cache{datum_index = DatumIndex} = State) ->

    %% Send a 'last_active' request to all current cache datum pids...
    Ref = make_ref(),
    Request_Timestamp_Fn =
        fun({_UseKey, DatumPid, _Size}, Replies_Expected) ->
                DatumPid ! {last_active, Ref, self()},
                Replies_Expected + 1
        end,
    Datum_Count = ets:foldl(Request_Timestamp_Fn, 0, DatumIndex),

    %% Then filter the results coming back for the oldest one and destroy it.
    case filter_oldest(Ref, Datum_Count, calendar:time_to_seconds(now()), false) of
        false      -> no_oldest_datum;
        Oldest_Pid -> Oldest_Pid ! {destroy, Ref, self()}
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
  
handle_call(Catch_All, _From, State) ->
  {reply, {arbitrary, Catch_All}, State}.


%%%----------------------------------------------------------------------
%%% handle_cast messages
%%%----------------------------------------------------------------------

handle_cast({dirty, DatumKey, NewData},
            #cache{datum_index = DatumIndex, cache_used = Used} = State) ->
    UseKey = key(DatumKey),
    case ets:lookup(DatumIndex, UseKey) of
        [] -> {noreply, State};
        [{UseKey, DatumPid, Size}] ->
            DatumPid ! {new_data, make_ref(), self(), NewData},
            %% Cache size is incremented by handle_info new_datum_size message
            {noreply, State#cache{cache_used = Used - Size}}
    end;

handle_cast({dirty, DatumKey}, #cache{datum_index = DatumIndex} = State) ->
    UseKey = key(DatumKey),
    case ets:lookup(DatumIndex, UseKey) of
        [] -> noop;
        [{UseKey, DatumPid, _Size}] ->
            Ref = make_ref(),
            DatumPid ! {destroy, Ref, self()}
    end,
    %% Cache size is updated by handle_info 'DOWN' message
    {noreply, State};

handle_cast({generic_dirty, M, F, A},
    #cache{datum_index = DatumIndex} = State) ->
    UseKey = key(M, F, A),
    case ets:lookup(DatumIndex, UseKey) of
        [] -> noop;
        [{UseKey, DatumPid, _}] ->
            Ref = make_ref(),
            DatumPid ! {destroy, Ref, self()}
    end,
    %% Cache size is updated by handle_info 'DOWN' message
    {noreply, State}.


%%%----------------------------------------------------------------------
%%% handle_info messages
%%%----------------------------------------------------------------------

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
  erlang:monitor(process, NewReaperPid),
  {noreply, State#cache{reaper_pid = NewReaperPid}};

handle_info({'DOWN', _Ref, process, DatumPid, _Reason},
    #cache{datum_index = DatumIndex, cache_used = Used} = State) ->

  %% Remove the _one_ pending 'new_datum_size' message, if it exists...
  receive {new_datum_size, {_UseKey, DatumPid, _Size}} -> noop
  after 0 -> none
  end,

  %% There can only be 1 entry in an ets 'set' table, so don't scan full table.
  New_State =
        case ets:match_object(DatumIndex, {'_', DatumPid, '_'}, 1) of
            '$end_of_table' -> State;
            {[], _Cont_Fn}  -> State; 
            {[{UseKey, DatumPid, Size}], _Cont_Fn} ->
                ets:delete(DatumIndex, UseKey),
                State#cache{cache_used = Used - Size}
        end,
  {noreply, New_State};

handle_info(_Info, State) ->
  %% io:format("Other info of: ~p~n", [Info]),
  {noreply, State}.


%% ===================================================================
%% Private
%% ===================================================================

get_age(DatumPid) ->
    Ref = make_ref(),
    DatumPid ! {last_active, Ref, self()},
    receive {last_active, Ref, DatumPid, Last_Active} -> {Last_Active, DatumPid}
    after 20 -> no_response
    end.
    
filter_oldest(_Ref, 0, _Oldest_Active, Oldest_Pid) -> Oldest_Pid;
filter_oldest( Ref, N,  Oldest_Active, Oldest_Pid) ->
    receive
        {last_active, Ref, DatumPid, Last_Active} ->
            case Last_Active < Oldest_Active of
                true  -> filter_oldest(Ref, N-1, Last_Active,   DatumPid);
                false -> filter_oldest(Ref, N-1, Oldest_Active, Oldest_Pid)
            end
    after 50 -> Oldest_Pid
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

-record(datum, {
          key            :: any(),
          mgr            :: pid(),
          data           :: any(),
          started        :: pos_integer(),
          last_active    :: pos_integer(),
          ttl            :: pos_integer(),
          type           :: cache_policy(),
          remaining_ttl  :: non_neg_integer()
         }).

create_datum(Cache_Server, Key, Data, TTL, Type) ->
  #datum{key = Key, mgr = Cache_Server, data = Data, ttl = TTL,
         remaining_ttl = TTL, type = Type, started = calendar:time_to_seconds(now())}.

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

datum_launch(Cache_Server, Key, UseKey, Module, Accessor, TTL, CachePolicy) ->
    Datum = make_new_datum(Cache_Server, Key, UseKey, Module, Accessor, TTL, CachePolicy),
    Datum_Pid = self(),
    {memory, Size} = process_info(Datum_Pid, memory),
    Cache_Server ! {new_datum_size, {UseKey, Datum_Pid, Size}},
    datum_loop(Datum).


%%% =======================================================================
%%% Datum pid internal state management and main receive loop
%%% =======================================================================

update_ttl(#datum{started = Started, ttl = TTL, type = actual_time} = Datum) ->

    %% Get total time in seconds this datum has been running.  Convert to ms.
    Now = calendar:time_to_seconds(now()),
    Started_Now_Diff = (Now - Started) * 1000,

    %% If we are less than the TTL, update with TTL-used (TTL in ms too)
    %% else, we ran out of time.  expire on next loop.
    TTL_Remaining = case TTL - Started_Now_Diff of
                        Remainder when Remainder > 0 -> Remainder;
                        _ -> 0
                    end,
    Datum#datum{last_active = Now, remaining_ttl = TTL_Remaining};

update_ttl(#datum{} = Datum) ->
    Datum#datum{last_active = calendar:time_to_seconds(now())}.

update_data(#datum{} = Datum, NewData) ->
  Datum#datum{data = NewData}.
    
continue(#datum{} = Datum) ->
  datum_loop(update_ttl(Datum)).

continue_noreset(#datum{} = Datum) ->
  datum_loop(Datum).

%% Give process a chance to discard old #datum{} before notifying.
datum_size_notify(Mgr, #datum{key = Key} = Datum) ->
    garbage_collect(),
    {memory, Size} = process_info(self(), memory),
    Mgr ! {new_datum_size, {Key, self(), Size}},
    datum_loop(Datum).

%% Main loop for datum replies and updates.    
datum_loop(#datum{key = Key, mgr = Mgr, last_active = LastActive,
            data = Data, remaining_ttl = TTL} = State) ->
  receive
    {new_data, _Ref, Mgr, Replacement} ->
      New_Datum = update_data(State, Replacement),
      datum_size_notify(Mgr, update_ttl(New_Datum));

    %% Presumably bad code elsewhere, but intent is for datum to be activated...
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
      continue_noreset(State);

    {destroy, Ref, From} ->
      From ! {destroy, Ref, self(), ok},
      % io:format("destroying ~p with last access of ~p~n", [self(), LastActive]),
      exit(self(), destroy);

    %% Presumably bad code elsewhere, but intent is for datum to be activated...
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

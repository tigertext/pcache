-module(pcache_server).

-behaviour(gen_server).

-export([start_link/3, start_link/4, start_link/5, start_link/6, start_link/7]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

%% Spawned functions
-export([datum_launch/7, datum_loop/1]).

-type cache_policy() :: lru | actual_time.

-record(cache, {
          name               :: atom(),
          index_type   = ets :: ets | pdict,
          datum_index        :: ets:tab() | pdict,
          data_module        :: module(),
          reaper_pid         :: pid(),
          data_accessor      :: atom(),
          cache_size   = 0   :: pos_integer(),
          cache_policy = lru :: cache_policy(),
          default_ttl  = 0   :: pos_integer(),
          cache_used   = 0   :: non_neg_integer()
         }).

% make 8 MB cache
start_link(Name, Mod, Fun) ->
  start_link(Name, Mod, Fun, 8).

% make 5 minute expiry cache
start_link(Name, Mod, Fun, CacheSize) ->
  start_link(Name, Mod, Fun, CacheSize, 300000).

% make LRU policy cache
start_link(Name, Mod, Fun, CacheSize, CacheTime) ->
  start_link(Name, Mod, Fun, CacheSize, CacheTime, lru).

start_link(Name, Mod, Fun, CacheSize, CacheTime, CachePolicy)
  when CachePolicy =:= lru; CachePolicy =:= actual_time ->
    start_link(Name, Mod, Fun, CacheSize, CacheTime, CachePolicy, ets).

start_link(Name, Mod, Fun, CacheSize, CacheTime, CachePolicy, Index_Type) ->
    Args = {Name, Mod, Fun, CacheSize, CacheTime, CachePolicy, Index_Type},
    gen_server:start_link({local, Name}, ?MODULE, Args, []).


%%%----------------------------------------------------------------------
%%% Init and gen_server state, terminate, code_change and locate functions
%%%----------------------------------------------------------------------

init({Name, Mod, Fun, CacheSize, CacheTime, CachePolicy, Index_Type})
  when Index_Type =:= ets; Index_Type =:= pdict ->
    DatumIndex = create_index(Index_Type, Name),
    CacheSizeBytes = CacheSize*1024*1024,

    {ok, ReaperPid} = pcache_reaper:start(Name, CacheSizeBytes),
    erlang:monitor(process, ReaperPid),

    State = #cache{name          = Name,
                   index_type    = Index_Type,
                   datum_index   = DatumIndex,
                   data_module   = Mod,
                   data_accessor = Fun,
                   reaper_pid    = ReaperPid,
                   default_ttl   = CacheTime,
                   cache_policy  = CachePolicy,
                   cache_size    = CacheSizeBytes,
                   cache_used    = 0},
    {ok, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

%% locate(DatumKey, #cache{index_type = pdict,      datum_index = DatumIndex, data_module = DataModule,
%%                         default_ttl = DefaultTTL, cache_policy = Policy, data_accessor = DataAccessor}) ->
%%   UseKey = key(DatumKey),
%%   case index_lookup(Index_Type, DatumIndex, UseKey) of 
%%     [{UseKey, Value, _Size}] -> Value;
%%     [] -> launch_datum(Index_Type, DatumKey, UseKey, DatumIndex, DataModule, DataAccessor, DefaultTTL, Policy)
%%   end;
locate(DatumKey, #cache{index_type = Index_Type, datum_index = DatumIndex, data_module = DataModule,
                        default_ttl = DefaultTTL, cache_policy = Policy, data_accessor = DataAccessor}) ->
  UseKey = key(DatumKey),
    error_logger:error_msg("Checking index: ~p ~p ~p~n", [Index_Type, DatumIndex, UseKey]),
  case index_lookup(Index_Type, DatumIndex, UseKey) of 
    [{UseKey, Pid, _Size}] when is_pid(Pid) -> Pid;
    [] -> launch_datum(Index_Type, DatumKey, UseKey, DatumIndex, DataModule, DataAccessor, DefaultTTL, Policy)
  end.

locate_memoize(Index_Type, DatumKey, DatumIndex, DataModule, DataAccessor, DefaultTTL, Policy) ->
  UseKey = key(DataModule, DataAccessor, DatumKey),
  case index_lookup(Index_Type, DatumIndex, UseKey) of 
    [{UseKey, Pid, _Size}] when is_pid(Pid) -> Pid;
    [] -> launch_memoize_datum(Index_Type, DatumKey, UseKey, DatumIndex, DataModule, DataAccessor, DefaultTTL, Policy)
  end.

key(Key) -> Key.
key(M, F, A) -> {M, F, A}.


%%%----------------------------------------------------------------------
%%% handle_call messages
%%%----------------------------------------------------------------------

%% Only to allow for testing of reap_oldest and performance
handle_call({age, Key}, _From, #cache{} = State) ->
    Datum_Pid = locate(Key, State),
    Age = get_age(Datum_Pid),
    {reply, Age, State};

handle_call(ages, _From, #cache{index_type = Index_Type, datum_index = DatumIndex} = State) ->
    Get_Pids_Fn = fun({_UseKey, DatumPid, _Size}, Pids) -> [DatumPid | Pids] end,
    All_Datums = index_foldl(Index_Type, Get_Pids_Fn, [], DatumIndex),
    Ages = [get_age(Pid) || Pid <- All_Datums],
    {reply, Ages, State};

handle_call({change_default_ttl, New_TTL}, _From, #cache{default_ttl = Old_TTL} = State) ->
    {reply, Old_TTL, State#cache{default_ttl = New_TTL}};

handle_call({location, DatumKey}, _From, State) -> 
  {reply, locate(DatumKey, State), State};

handle_call({generic_get, M, F, Key}, From,
            #cache{index_type = Index_Type, datum_index = DatumIndex, default_ttl = DefaultTTL,
                   cache_policy = Policy} = State) ->
    %% io:format("Requesting: ~p:~p(~p)~n", [M, F, Key]),
    DatumPid = locate_memoize(Index_Type, Key, DatumIndex, M, F, DefaultTTL, Policy),
    DatumPid ! {get, From},
    {noreply, State};

handle_call({get, Key}, From, #cache{} = State) ->
    %% io:format("Requesting: (~p)~n", [Key]),
    DatumPid = locate(Key, State),
    DatumPid ! {get, From},
    {noreply, State};

handle_call(total_size, _From, #cache{cache_used = Used} = State) ->
  {reply, Used, State};

handle_call(stats, _From, #cache{name = Cache_Name, index_type = Index_Type, datum_index = Datum_Index,
                                 cache_used = Used, cache_size = Size, default_ttl = Default_TTL} = State) ->
    Stats = [
             {cache_name,  Cache_Name}, {datum_count, index_count(Index_Type, Datum_Index)},
             {memory_used, Used},       {memory_allocated, Size},
             {default_ttl, Default_TTL}
            ],
    {reply, Stats, State};

handle_call(empty, _From, #cache{index_type = Index_Type, datum_index = DatumIndex} = State) ->
    %% We don't wait synchronously, so the make_ref() is just for a consistent interface message.
    Ref = make_ref(),
    Destroy_Fn = fun({DatumKey, DatumPid, _Size}, Items) ->
                         DatumPid ! {destroy, Ref, self()},
                         [DatumKey | Items]
                 end,
    Items_Destroyed = index_foldl(Index_Type, Destroy_Fn, [], DatumIndex),
    _ = [index_delete(Index_Type, DatumIndex, Key) || Key <- Items_Destroyed],
    {reply, length(Items_Destroyed), State#cache{cache_used = 0}};

handle_call(reap_oldest, _From, #cache{index_type = Index_Type, datum_index = DatumIndex} = State) ->

    %% Send a 'last_active' request to all current cache datum pids...
    Ref = make_ref(),
    Request_Timestamp_Fn = fun({_UseKey, DatumPid, _Size}, Replies_Expected) ->
                                   DatumPid ! {last_active, Ref, self()},
                                   Replies_Expected + 1
                           end,
    Datum_Count = index_foldl(Index_Type, Request_Timestamp_Fn, 0, DatumIndex),

    %% Then filter the results coming back for the oldest one and destroy it.
    NewState = case filter_oldest(Ref, Datum_Count, erlang:now(), false) of
                   false -> State;
                   Oldest_Pid ->
                       Oldest_Pid ! {destroy, Ref, self()},
                       %% There can only be 1 entry in an ets 'set' table, so don't scan full table.
                       delete_one_ets_entry_by_pid(DatumIndex, Oldest_Pid, State)
               end,
    {reply, ok, NewState};

handle_call({rand, Type, Num_Desired}, _From, #cache{index_type = Index_Type, datum_index = Datum_Index} = State) ->
    Datum_Count = index_count(Index_Type, Datum_Index),
    Indices = [crypto:rand_uniform(1, Datum_Count+1) || _ <- lists:seq(1, Num_Desired)],
    Rand_Fn = fun(_Ets_Entry, {Pids, Ets_Item_Pos, []}) -> {Pids, Ets_Item_Pos+1, []};
                 ({_UseKey, Datum_Pid, _Size}, {Pids, Ets_Item_Pos, Wanted_Pid_Positions}) ->
                      Fetch_Positions = lists:takewhile(fun(Pos) -> Pos =:= Ets_Item_Pos end, Wanted_Pid_Positions),
                      Fetch_Pids = lists:duplicate(length(Fetch_Positions), Datum_Pid),
                      {Fetch_Pids ++ Pids, Ets_Item_Pos+1, Wanted_Pid_Positions -- Fetch_Positions}
              end,
    {Rand_Pids, _Last_Pos, []} = index_foldl(Index_Type, Rand_Fn, {[], 1, lists:sort(Indices)}, Datum_Index),
    Rand_Data = case Type of
                     data -> [get_data(P) || P <- Rand_Pids];
                     keys -> [get_key(P)  || P <- Rand_Pids]
                 end,
    {reply, Rand_Data, State};
  
handle_call(Catch_All, _From, State) ->
    {reply, {arbitrary, Catch_All}, State}.


%%%----------------------------------------------------------------------
%%% handle_cast messages
%%%----------------------------------------------------------------------

handle_cast({dirty, DatumKey, NewData},
            #cache{index_type = Index_Type, datum_index = DatumIndex, cache_used = Used} = State) ->
    UseKey = key(DatumKey),
    case index_lookup(Index_Type, DatumIndex, UseKey) of
        [] -> {noreply, State};
        [{UseKey, DatumPid, Size}] ->
            DatumPid ! {new_data, make_ref(), self(), NewData},
            %% Pid doesn't change, so no update to cache index
            %% Cache size is incremented by handle_info new_datum_size message
            {noreply, State#cache{cache_used = Used - Size}}
    end;

handle_cast({dirty, DatumKey},
            #cache{index_type = Index_Type, datum_index = DatumIndex, cache_used = Used} = State) ->
    UseKey = key(DatumKey),
    case index_lookup(Index_Type, DatumIndex, UseKey) of
        [] -> {noreply, State};

        %% Atomically update the ets / size / pid
        [{UseKey, DatumPid, Size}] ->
            Ref = make_ref(),
            DatumPid ! {destroy, Ref, self()},
            index_delete(Index_Type, DatumIndex, UseKey),
            {noreply, State#cache{cache_used = Used - Size}}
    end;

handle_cast({generic_dirty, M, F, A},
            #cache{index_type = Index_Type, datum_index = DatumIndex, cache_used = Used} = State) ->
    UseKey = key(M, F, A),
    case index_lookup(Index_Type, DatumIndex, UseKey) of
        [] -> {noreply, State};

        %% Atomically update the ets / size / pid
        [{UseKey, DatumPid, Size}] ->
            Ref = make_ref(),
            DatumPid ! {destroy, Ref, self()},
            index_delete(Index_Type, DatumIndex, UseKey),
            {noreply, State#cache{cache_used = Used - Size}}
    end;

handle_cast({expire, Pct_TTL_Remaining}, #cache{index_type = Index_Type, datum_index = Datum_Index} = State) ->
    Ref = make_ref(),
    Notify_All_Fn = fun({_UseKey, Datum_Pid, _Size}, N) -> Datum_Pid ! {{expire, Pct_TTL_Remaining}, Ref, Ref}, N+1 end,
    index_foldl(Index_Type, Notify_All_Fn, 0, Datum_Index),
    {noreply, State}.


%%%----------------------------------------------------------------------
%%% handle_info messages
%%%----------------------------------------------------------------------

%% New datum is inserted at 0 size initially, then updated with real size by a message.
handle_info({new_datum_size, {UseKey, DatumPid, Size}},
            #cache{index_type = Index_Type, datum_index = DatumIndex, cache_used = Used} = State) ->
  index_insert(Index_Type, DatumIndex, UseKey, {UseKey, DatumPid, Size}),
  {noreply, State#cache{cache_used = Used + Size}};

handle_info({destroy, _Ref, _DatumPid, ok}, State) ->
  {noreply, State};

handle_info({'DOWN', _Ref, process, ReaperPid, _Reason}, 
    #cache{reaper_pid = ReaperPid, name = Name, cache_size = Size} = State) ->
  {NewReaperPid, _Mon} = pcache_reaper:start_link(Name, Size),
  erlang:monitor(process, NewReaperPid),
  {noreply, State#cache{reaper_pid = NewReaperPid}};

handle_info({'DOWN', _Ref, process, DatumPid, _Reason},
    #cache{datum_index = DatumIndex} = State) ->

  %% Remove the _one_ pending 'new_datum_size' message, if it exists...
  receive {new_datum_size, {_UseKey, DatumPid, _Size}} -> noop
  after 0 -> none
  end,

  %% There can only be 1 entry in an ets 'set' table, so don't scan full table.
  New_State = delete_one_ets_entry_by_pid(DatumIndex, DatumPid, State),
  {noreply, New_State};

handle_info(_Info, State) ->
  %% io:format("Other info of: ~p~n", [Info]),
  {noreply, State}.


%% ===================================================================
%% Private
%% ===================================================================

delete_one_ets_entry_by_pid(DatumIndex, DatumPid, #cache{index_type = Index_Type, cache_used = Used} = State) ->
    case ets:match_object(DatumIndex, {'_', DatumPid, '_'}, 1) of
        '$end_of_table' -> State;
        {[], _Cont_Fn}  -> State; 
        {[{UseKey, DatumPid, Size}], _Cont_Fn} ->
            index_delete(Index_Type, DatumIndex, UseKey),
            State#cache{cache_used = Used - Size}
    end.

get_age(DatumPid) ->
    Ref = make_ref(),
    DatumPid ! {last_active, Ref, self()},
    receive {last_active, Ref, DatumPid, Last_Active} -> {Last_Active, DatumPid}
    after 50 -> no_response
    end.
    
filter_oldest(_Ref, 0, _Oldest_Active, Oldest_Pid) -> Oldest_Pid;
filter_oldest( Ref, N,  Oldest_Active, Oldest_Pid) ->
    receive
        {last_active, Ref, DatumPid, Last_Active} ->
            case Last_Active < Oldest_Active of
                true  -> filter_oldest(Ref, N-1, Last_Active,   DatumPid);
                false -> filter_oldest(Ref, N-1, Oldest_Active, Oldest_Pid)
            end
    after 30 -> Oldest_Pid
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
          key                          :: any(),
          mgr                          :: pid(),
          data                         :: any(),
          policy_type   = lru          :: cache_policy(),
          started       = erlang:now() :: erlang:timestamp(),
          last_active   = erlang:now() :: erlang:timestamp(),
          ttl           = 0            :: pos_integer(),       %% Number of milliseconds
          remaining_ttl = 0            :: non_neg_integer()    %% Number of milliseconds
         }).

create_datum(Cache_Server, Key, Data, TTL, Type) ->
    #datum{key = Key, mgr = Cache_Server, data = Data, policy_type = Type, ttl = TTL, remaining_ttl = TTL}.

make_new_datum(Cache_Server, Key, UseKey, Module, Accessor, TTL, CachePolicy) ->
    try
        CacheData = Module:Accessor(Key),
        create_datum(Cache_Server, UseKey, CacheData, TTL, CachePolicy)
    catch Class:Error ->
            Crash_Args = [Module, Accessor, Key, Class, Error, erlang:get_stacktrace()],
            Crash_String = lists:flatten(io_lib:format("** ~p:~p(~p) Crashed! ~p:~p **~nStacktrace:~p~n", Crash_Args)),
            error_logger:error_msg("~p:make_new_datum ~p~n", [?MODULE, Crash_String]),
            exit(crashed)
    end.

%% launch_datum(pdict, DatumKey, UseKey, DatumIndex, Module, Accessor, TTL, CachePolicy) ->
%%   Datum_Args = [self(), DatumKey, UseKey, Module, Accessor, TTL, CachePolicy],
%%   {Datum_Pid, _Monitor} = erlang:spawn_monitor(?MODULE, datum_launch, Datum_Args),
%%   index_insert(Index_Type, DatumIndex, UseKey, {UseKey, Datum_Pid, 0}),
%%   Datum_Pid.
launch_datum(Index_Type, DatumKey, UseKey, DatumIndex, Module, Accessor, TTL, CachePolicy) ->
  Datum_Args = [self(), DatumKey, UseKey, Module, Accessor, TTL, CachePolicy],
    error_logger:error_msg("Launching datum: ~p~n", [Datum_Args]),
  {Datum_Pid, _Monitor} = erlang:spawn_monitor(?MODULE, datum_launch, Datum_Args),
    error_logger:error_msg("Datum launched: ~p~n", [Datum_Pid]),
  index_insert(Index_Type, DatumIndex, UseKey, {UseKey, Datum_Pid, 0}),
  Datum_Pid.

launch_memoize_datum(Index_Type, DatumKey, UseKey, DatumIndex, Module, Accessor, TTL, CachePolicy) ->
  Datum_Args = [self(), DatumKey, UseKey, Module, Accessor, TTL, CachePolicy],
  {Datum_Pid, _Monitor} = erlang:spawn_monitor(?MODULE, datum_launch, Datum_Args),
  index_insert(Index_Type, DatumIndex, UseKey, {UseKey, Datum_Pid, 0}),
  Datum_Pid.

datum_launch(Cache_Server, Key, UseKey, Module, Accessor, TTL, CachePolicy) ->
    Datum = make_new_datum(Cache_Server, Key, UseKey, Module, Accessor, TTL, CachePolicy),
    datum_size_notify(Cache_Server, Datum).


%%% =======================================================================
%%% Datum pid internal state management and main receive loop
%%% =======================================================================

%% Expire on next loop if TTL has passed since process started when using 'actual_time'.
update_ttl(#datum{started = Started, ttl = TTL, policy_type = actual_time} = Datum) ->
    Now = erlang:now(),
    Elapsed = timer:now_diff(Now, Started) div 1000,
    TTL_Remaining = case TTL - Elapsed of
                        Remainder when Remainder > 0 -> Remainder;
                        _ -> 0
                    end,
    Datum#datum{last_active = Now, remaining_ttl = TTL_Remaining};

%% Expire after TTL of idle time on all other cache strategies.
update_ttl(#datum{} = Datum) ->
    Datum#datum{last_active = erlang:now()}.


update_data(#datum{} = Datum, NewData) ->
  Datum#datum{data = NewData}.
    
continue        (#datum{} = Datum) -> datum_loop(update_ttl(Datum)).
continue_noreset(#datum{} = Datum) -> datum_loop(Datum).

%% Give process a chance to discard old #datum{} before notifying.
datum_size_notify(Mgr, #datum{key = Key} = Datum) ->
    Datum_Pid = self(),
    garbage_collect(),
    {memory, Size} = process_info(Datum_Pid, memory),
    Mgr ! {new_datum_size, {Key, Datum_Pid, Size}},
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

    %% Get messages used to fulfill pcache_server:handle_call requests...
    {get, Gen_Server_From} ->
      gen_server:reply(Gen_Server_From, Data),
      continue(State);

    {getkey, Gen_Server_From} ->
      gen_server:reply(Gen_Server_From, Key),
      continue(State);

    %% Get messages used to fulfill pcache_server:rand collection of values...
    {get, Ref, From} ->
      From ! {get, Ref, self(), Data},
      continue(State);

    {getkey, Ref, From} ->
      From ! {getkey, Ref, self(), Key},
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

    %% Request to expire if less than Pct_TTL_Remaining...
    {{expire, Pct_TTL_Remaining}, _Ref, _From}
        when is_integer(Pct_TTL_Remaining), Pct_TTL_Remaining > 0, Pct_TTL_Remaining < 100 ->
          Now = erlang:now(),
          Time_Buffer = (Pct_TTL_Remaining * TTL div 100),
          Elapsed = timer:now_diff(Now, State#datum.started) div 1000,
          Remaining_TTL = case State#datum.policy_type of
                              actual_time -> Elapsed - Time_Buffer;
                              _Other      -> TTL - Elapsed - Time_Buffer
                          end,
          %% error_logger:info_msg("Expire: ~p (~p vs ~p)~n", [Key, Remaining_TTL, Time_Buffer]),
          case Remaining_TTL of
              Expired when Expired =< 0 -> cache_is_now_dead;
              _Not_Expired -> continue_noreset(State)
          end;

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


%%%----------------------------------------------------------------------
%%% Generalize index for datum pids to allow alternatives to ets
%%%----------------------------------------------------------------------

create_index(ets,    Name) -> ets:new(Name, [set, private]);
create_index(pdict, _Name) -> pdict.

index_count(ets, Ets_Table) -> ets:info(Ets_Table, size);
index_count(pdict, pdict)   -> length(get()).   %% This is very inefficient

index_insert(ets, Datum_Index, _Key, Value) -> ets:insert(Datum_Index, Value);
index_insert(pdict, pdict, Key, Value)      -> put(Key, Value), true.

index_lookup(ets, Datum_Index, Key) -> ets:lookup(Datum_Index, Key);
index_lookup(pdict, pdict, Key)     -> get(Key).

index_delete(ets, Datum_Index, Key) -> ets:delete(Datum_Index, Key);
index_delete(pdict, pdict, Key)     -> erase(Key), true.

index_foldl(ets, Fun, Init_Acc, Datum_Index) ->
    ets:foldl(Fun, Init_Acc, Datum_Index);
index_foldl(pdict, Fun, Init_Acc, pdict) ->
    lists:foldl(Fun, Init_Acc, get()).

%% index_delete_by_pid(Datum_Index, Datum_Pid, #cache{index_type=ets, cache_used = Used} = State) ->
%%     case ets:match_object(Datum_Index, {'_', Datum_Pid, '_'}, 1) of
%%         '$end_of_table' -> State;
%%         {[], _Cont_Fn}  -> State; 
%%         {[{Use_Key, Datum_Pid, Size}], _Cont_Fn} ->
%%             ets:delete(Datum_Index, Use_Key),
%%             State#cache{cache_used = Used - Size}
%%     end;
%% index_delete_by_pid(_Datum_Index, Datum_Pid, #cache{index_type=pdict, cache_used = Used} = State) ->
%%     case get(Datum_Pid) of
%%         undefined -> State;
%%         Key       -> {Use_Key, Datum_Pid, Size} = get(Key),
%%                      erase(Datum_Pid),
%%                      erase(Key),
%%                      State#cache{cache_used = Used - Size}
%%     end.

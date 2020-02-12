%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Default plugin callbacks
-module(nkactor_store_pgsql_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').


-export([actor_store_pgsql_parse/4, actor_store_pgsql_unparse/4]).
-export([status/1]).
-export([actor_db_init/1,
         actor_db_find/3, actor_db_read/3, actor_db_create/3, actor_db_update/3,
         actor_db_delete/3, actor_db_delete_multi/3, actor_db_search/3, actor_db_aggregate/3,
         actor_db_truncate/2]).

-include("nkactor_store_pgsql.hrl").
-include_lib("nkserver/include/nkserver.hrl").


%% ===================================================================
%% Offered callbacks
%% ===================================================================


%% @doc Called after reading the actor, to process further de-serializations
-spec actor_store_pgsql_parse(nkserver:id(), nkactor:actor(), map(), db_opts()) ->
    {ok, nkactor:actor(), map()} | {error, term()}.

actor_store_pgsql_parse(_SrvId, Actor, Meta, _Opts) ->
    {ok, Actor, Meta}.


%% @doc Called before saving the actor, to process further serializations
-spec actor_store_pgsql_unparse(nkserver:id(), nkactor:actor(), create|updated, db_opts()) ->
    {ok, nkactor:actor()} | {error, term()}.

actor_store_pgsql_unparse(_SrvId, _Op, Actor, _Opts) ->
    {ok, Actor}.



%% ===================================================================
%% Status
%% ===================================================================

status({pgsql_error, Error}) -> {"PgSQL Error: ~p", [Error]};
status(_) -> continue.



%% ===================================================================
%% Persistence callbacks
%% ===================================================================

-type id() :: nkserver:id().
-type actor_id() :: nkactor:actor_id().
-type actor() :: nkactor:actor().

-type continue() :: nkserver_callbacks:continue().

-type db_opts() :: nkactor_callbacks:db_opts().


%% @doc Called after the core has initialized the database
-spec actor_db_init(nkserver:id()) ->
    ok | {error, term()} | continue().

actor_db_init(_SrvId) ->
    ok.


%% @doc Must find an actor on disk by UID (if available) or name, and return
%% full actor_id data
-spec actor_db_find(id(), actor_id(), db_opts()) ->
    {ok, actor_id(), Meta::map()} | {error, actor_not_found|term()} | continue().

actor_db_find(SrvId, ActorId, Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() -> nkactor_store_pgsql_actors:find(PgSrvId, ActorId, Opts) end,
            trace_new(SrvId, PgSrvId, find, Fun)
    end.


%% @doc Must find and read a full actor on disk by UID (if available) or name
-spec actor_db_read(id(), actor_id(), db_opts()) ->
    {ok, nkactor:actor(), Meta::map()} | {error, actor_not_found|term()} | continue().

actor_db_read(SrvId, ActorId, Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() ->
                case nkactor_store_pgsql_actors:read(PgSrvId, ActorId, Opts) of
                    {ok, RawActor, Meta} ->
                        parse_actor(SrvId, RawActor, Meta, Opts);
                    {error, Error} ->
                        {error, Error}
                end
            end,
            trace_new(SrvId, PgSrvId, read, Fun)
    end.


%% @doc Must create a new actor on disk. Should fail if already present
-spec actor_db_create(id(), actor(), db_opts()) ->
    {ok, Meta::map()} | {error, uniqueness_violation|term()} | continue().

actor_db_create(SrvId, Actor, Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() ->
                case
                    ?CALL_SRV(SrvId, actor_store_pgsql_unparse, [SrvId, create, Actor, Opts])
                of
                    {ok, Actor2} ->
                        nkactor_store_pgsql_actors:create(PgSrvId, Actor2, Opts);
                    {error, Error} ->
                        {error, Error}
                end
            end,
            trace_new(SrvId, PgSrvId, create, Fun)
    end.


%% @doc Must update a new actor on disk.
-spec actor_db_update(id(), actor(), db_opts()) ->
    {ok, Meta::map()} | {error, term()} | continue().

actor_db_update(SrvId, Actor, Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() ->
                case
                    ?CALL_SRV(SrvId, actor_store_pgsql_unparse, [SrvId, update, Actor, Opts])
                of
                    {ok, Actor2} ->
                        nkactor_store_pgsql_actors:update(PgSrvId, Actor2, Opts);
                    {error, Error} ->
                        {error, Error}
                end
            end,
            trace_new(SrvId, PgSrvId, update, Fun)
    end.


%% @doc
-spec actor_db_delete(id(), actor_id(), db_opts()) ->
    {ok, Meta::map()} | {error, term()} | continue().

actor_db_delete(SrvId, ActorId, Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() -> nkactor_store_pgsql_actors:delete(PgSrvId, ActorId, Opts) end,
            trace_new(SrvId, PgSrvId, delete, Fun)
    end.


%% @doc
-spec actor_db_delete_multi(id(), [actor_id()], db_opts()) ->
    {ok, #{deleted:=integer()}} | {error, term()} | continue().

actor_db_delete_multi(SrvId, ActorIds, Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() -> nkactor_store_pgsql_actors:delete_multi(PgSrvId, ActorIds, Opts) end,
            trace_new(SrvId, PgSrvId, delete_multi, Fun)
    end.


%% @doc
-spec actor_db_search(id(), nkactor_backend:search_type(), db_opts()) ->
    {ok, [actor()], Meta::map()} | {error, term()} | continue().

actor_db_search(SrvId, Type, Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() ->
                case nkactor_store_pgsql_search:search(Type, Opts) of
                    {query, Query, Fun} ->
                        Opts2 = #{result_fun=>Fun, nkactor_params=>Opts},
                        case nkactor_store_pgsql:query(PgSrvId, Query, Opts2) of
                            {ok, ActorList, Meta} ->
                                parse_actors(ActorList, SrvId, Meta, Opts, []);
                            {error, Error} ->
                                {error, Error}
                        end;
                    {error, Error} ->
                        {error, Error}
                end
            end,
            trace_new(SrvId, PgSrvId, <<"search">>, Fun)
    end.


%% @doc
-spec actor_db_aggregate(id(), nkactor_backend:agg_type(), db_opts()) ->
    {ok, [actor()], Meta::map()} | {error, term()} | continue().

actor_db_aggregate(SrvId, Type, Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            Fun = fun() ->
                case nkactor_store_pgsql_aggregation:aggregation(Type, Opts) of
                    {query, Query, Fun} ->
                        nkactor_store_pgsql:query(PgSrvId, Query, #{result_fun=>Fun});
                    {error, Error} ->
                        {error, Error}
                end
            end,
            trace_new(SrvId, PgSrvId, <<"aggregate">>, Fun)
    end.


%% @doc
-spec actor_db_truncate(id(), db_opts()) ->
    ok | {error, term()} | continue().

actor_db_truncate(SrvId, _Opts) ->
    case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
        undefined ->
            continue;
        PgSrvId ->
            nkactor_store_pgsql_init:truncate(PgSrvId)
    end.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
trace_new(SrvId, PgSrvId, Op, Fun) ->
    Name = <<"ActorPgSQL::", (nklib_util:to_binary(Op))/binary>>,
    Opts = #{
        metadata => #{
            app => PgSrvId,
            group => actor_store_pgsql,
            resource => Op
        }
    },
    Fun2 = fun() -> reply(Fun()) end,
    nkserver_trace:new(SrvId, Name, Fun2, Opts).


%% @doc
parse_actor(SrvId, RawActor, Meta, Opts) ->
    nkserver_trace:trace("parsing actors"),
    case nkactor_syntax:parse_actor(RawActor, #{}) of
        {ok, Actor} ->
            ?CALL_SRV(SrvId, actor_store_pgsql_parse, [SrvId, Actor, Meta, Opts]);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
parse_actors([], _SrvId, Meta, _Opts, Acc) ->
    {ok, lists:reverse(Acc), Meta};

parse_actors([#{uid:=_}=RawActor|Rest], SrvId, Meta, Opts, Acc) ->
    case parse_actor(SrvId, RawActor, Meta, Opts) of
        {ok, Actor, Meta2} ->
            parse_actors(Rest, SrvId, Meta2, Opts, [Actor|Acc]);
        {error, Error} ->
            {error, Error}
    end;

parse_actors([Other|Rest], SrvId, Meta, Opts, Acc) ->
    parse_actors(Rest, SrvId, Meta, Opts, [Other|Acc]).



%% @private
reply({ok, Data, Meta}) ->
    nkserver_trace:trace("result: ~p", [Meta]),
    {ok, Data, Meta};

reply({error, Error}) when
    Error == duplicated_name;
    Error == actor_has_linked_actors;
    Error == actor_not_found;
    Error == uniqueness_violation ->
    {error, Error};

reply({error, Error}) ->
    nkserver_trace:log(notice, "PgSSQL error: ~p", [Error]),
    nkserver_trace:error(Error),
    {error, {pgsql_error, Error}}.




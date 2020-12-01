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

-module(nkactor_store_pgsql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([get_pgsql_srv/1]).
-export([query/2, query/3]).
-export([quote/1, filter_path/1]).
-export_type([result_fun/0]).

-include("nkactor_store_pgsql.hrl").

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR PGSQL "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================

-type result_fun() :: fun(([[tuple()]], map()) -> {ok, term(), map()} | {error, term()}).


%% ===================================================================
%% API
%% ===================================================================

% https://www.cockroachlabs.com/docs/stable/
% https://www.cockroachlabs.com/docs/dev/

%% @doc
get_pgsql_srv(ActorSrvId) ->
    nkserver:get_cached_config(ActorSrvId, nkactor_store_pgsql, pgsql_service).


%% @doc Performs a query. Must use the PgSQL service
-spec query(nkserver:id(), binary()|nkpgsql:query_fun()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, nkpgsql:pgsql_error()}|term()}.

query(SrvId, Query) ->
    query(SrvId, Query, #{}).


%% @doc Performs a query. Must use the PgSQL service
-spec query(nkserver:id(), binary()|nkpgsql:query_fun(), nkpgsql:query_meta()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, nkpgsql:pgsql_error()}|term()}.

query(SrvId, Query, QueryMeta) ->
    QueryBin = list_to_binary([Query]),
    Tag = case byte_size(QueryBin) < 4096 of
        true ->
            QueryBin;
        false ->
            <<Tag0:4000/binary, _/binary>> = QueryBin,
            <<Tag0/binary, "...">>
    end,
    nkserver_trace:event(sql, "~s", [Tag], #{}),
    nkpgsql:query(SrvId, QueryBin, QueryMeta).




%% ===================================================================
%% Utilities
%% ===================================================================


quote(Term) ->
    nkpgsql_util:quote(Term).


filter_path(Opts) ->
    Namespace = maps:get(namespace, Opts, <<>>),
    Deep = maps:get(deep, Opts, false),
    filter_path(Namespace, Deep, Opts).


%% @private
filter_path(<<>>, true, _Opts) ->
    [<<"TRUE">>];

filter_path(Namespace, Deep, Opts) ->
    Path = nkactor_lib:make_rev_path(Namespace),
    Field = case Opts of
        #{use_labels:=true} -> <<"labels.path">>;
        _ -> <<"actors.path">>
    end,
    case Deep of
        true ->
            [<<"(", Field/binary, " LIKE ">>, quote(<<Path/binary, "%">>), <<")">>];
        false ->
            [<<"(", Field/binary, " = ">>, quote(Path), <<")">>]
    end.



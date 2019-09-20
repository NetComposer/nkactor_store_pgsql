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

-module(nkactor_store_pgsql_search).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([search/2]).
-export([pgsql_actors/2, pgsql_labels/2, pgsql_delete/2, pgsql_any/2]).
-import(nkactor_store_pgsql, [query/2, query/3, quote/1, filter_path/2]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR PGSQL "++Txt, Args)).

-include_lib("nkactor/include/nkactor.hrl").


%% ===================================================================
%% Search Types
%% ===================================================================


search(actors_search_linked, Params) ->
    UID = maps:get(uid, Params),
    LinkType = maps:get(link_type, Params, any),
    Namespace = maps:get(namespace, Params, <<>>),
    Deep = maps:get(deep, Params, false),
    From = maps:get(from, Params, 0),
    Limit = maps:get(size, Params, 100),
    Query = [
        <<"SELECT uid,link_type FROM links">>,
        <<" WHERE link_target=">>, quote(to_bin(UID)),
        case LinkType of
            any ->
                <<>>;
            _ ->
                [<<" AND link_type=">>, quote(LinkType)]
        end,
        <<" AND ">>, filter_path(Namespace, Deep),
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Limit),
        <<";">>
    ],
    ResultFun = fun(Ops, Meta) ->
        case Ops of
            [{{select, _}, [], _OpMeta}] ->
                {ok, [], Meta};
            [{{select, Size}, Rows, _OpMeta}] ->
                {ok, Rows, Meta#{size=>Size}}
        end
    end,
    {query, Query, ResultFun};


search(actors_search_fts, Params) ->
    Word = maps:get(word, Params),
    Field = maps:get(field, Params, any),
    Namespace = maps:get(namespace, Params, <<>>),
    Deep = maps:get(deep, Params, false),
    From = maps:get(from, Params, 0),
    Limit = maps:get(size, Params, 100),
    Word2 = nklib_parse:normalize(Word, #{unrecognized=>keep}),
    Last = byte_size(Word2)-1,
    Filter = case Word2 of
        <<Word3:Last/binary, $*>> ->
            [<<"fts_word LIKE ">>, quote(<<Word3/binary, $%>>)];
        _ ->
            [<<"fts_word=">>, quote(Word2)]
    end,
    Query = [
        <<"SELECT uid FROM fts">>,
        <<" WHERE ">>, Filter, <<" AND ">>, filter_path(Namespace, Deep),
        case Field of
            any ->
                [];
            _ ->
                [<<" AND fts_field = ">>, quote(Field)]
        end,
        <<" ORDER BY fts_word" >>,
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Limit),
        <<";">>
    ],
    ResultFun = fun([{{select, _}, List, _OpMeta}],Meta) ->
        List2 = [UID || {UID} <-List],
        {ok, List2, Meta}
    end,
    {query, Query, ResultFun};

search(actors_search, Params) ->
    case analyze(Params) of
        only_labels ->
            search(actors_search_labels, Params);
        generic ->
            search(actors_search_generic, Params)
    end;

search(actors_search_generic, Params) ->
    From = maps:get(from, Params, 0),
    Size = maps:get(size, Params, 10),
    Totals = maps:get(get_total, Params, false),
    SQLFilters = nkactor_store_pgsql_sql:filters(Params, actors),
    SQLSort = nkactor_store_pgsql_sql:sort(Params, actors),

    % We could use SELECT COUNT(*) OVER(),src,uid... but it doesn't work if no
    % rows are returned

    Query = [
        case Totals of
            true ->
                [
                    <<"SELECT COUNT(*) FROM actors">>,
                    SQLFilters,
                    <<";">>
                ];
            false ->
                []
        end,
        nkactor_store_pgsql_sql:select(Params, actors),
        SQLFilters,
        SQLSort,
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    {query, Query, fun ?MODULE:pgsql_actors/2};

search(actors_search_labels, Params) ->
    From = maps:get(from, Params, 0),
    Size = maps:get(size, Params, 10),
    Totals = maps:get(get_total, Params, false),
    SQLFilters = nkactor_store_pgsql_sql:filters(Params, labels),
    SQLSort = nkactor_store_pgsql_sql:sort(Params, labels),

    Query = [
        case Totals of
            true ->
                [
                    <<"SELECT COUNT(*) FROM labels">>,
                    SQLFilters,
                    <<";">>
                ];
            false ->
                []
        end,
        nkactor_store_pgsql_sql:select(Params, labels),
        SQLFilters,
        SQLSort,
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    {query, Query, fun ?MODULE:pgsql_labels/2};

search(actors_delete_old, Params) ->
    Group = maps:get(group, Params),
    Res = maps:get(resource, Params),
    Epoch = maps:get(epoch, params),
    Namespace = maps:get(namespace, Params, <<>>),
    Deep = maps:get(deep, Params, false),
    Query = [
        <<"DELETE FROM actors">>,
        <<" WHERE \"group\"=">>, quote(Group), <<" AND resource=">>, quote(Res),
        <<" AND last_update<">>, quote(Epoch),
        <<" AND ">>, filter_path(Namespace, Deep),
        <<";">>
    ],
    {query, Query, fun pgsql_delete/2};

% Find actors with activation date past or due in 2h from now
search(actors_activate, #{last_time:=LastTime}=Params) ->
    Size = maps:get(size, Params, 100),
    Query = [
        <<"SELECT uid,namespace,\"group\",resource,name,activate_time FROM actors">>,
        <<" WHERE activate_time < ">>, quote(LastTime),
        <<" ORDER BY activate_time DESC">>,
        <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    {query, Query, fun pgsql_activate/2};


search(SearchType, _Params) ->
    {error, {search_not_implemented, SearchType}}.



%% ===================================================================
%% Analyze
%% ===================================================================

%% @private
analyze(#{fields_filter:=Filter, fields_sort:=Sort, only_uid:=true}) ->
    case analyze_filter_labels(Filter, false) of
        true ->
            case analyze_filter_sort(Sort) of
                true ->
                    only_labels;
                false ->
                    generic
            end;
        false ->
            generic
    end;

analyze(_) ->
    generic.


%% @private
analyze_filter_labels([], Res) ->
    Res;

analyze_filter_labels([{<<"metadata.labels.", _/binary>>, _Op}|Rest], _Res) ->
    analyze_filter_labels(Rest, true);

analyze_filter_labels(_, _Res) ->
    false.


%% @private
analyze_filter_sort([]) ->
    true;

analyze_filter_sort([<<"metadata.labels", _/binary>>|Rest]) ->
    analyze_filter_sort(Rest);

analyze_filter_sort(_) ->
    false.




%% ===================================================================
%% Result funs
%% ===================================================================


%% @private
pgsql_actors(Result, Meta) ->
    % lager:error("NKLOG META ~p", [_Meta]),
    #{nkactor_params:=Params, pgsql:=#{time:=Time}} = Meta,
    {Rows, Meta2} = case Result of
        [{{select, Size}, Rows0, _OpMeta}] ->
            {Rows0, #{size=>Size, time=>Time}};
        [{{select, 1}, [{Total}], _}, {{select, Size}, Rows0, _OpMeta}] ->
            {Rows0, #{size=>Size, total=>Total, time=>Time}}
    end,
    GetData = maps:get(get_data, Params, false),
    GetMeta = maps:get(get_metadata, Params, false),
    Actors = lists:map(
        fun
            ({UID}) ->
                #{uid => UID};
            (Row) ->
                Actor1 = #{
                    uid => element(1, Row),
                    namespace => element(2, Row),
                    group => element(3, Row),
                    resource => element(4, Row),
                    name => element(5, Row)
                },
                Actor2 = case GetMeta of
                    true ->
                        {jsonb, MetaData} = element(6, Row),
                        Actor1#{metadata => nklib_json:decode(MetaData)};
                    false ->
                        Actor1
                end,
                Actor3 = case GetData of
                    true when GetMeta ->
                        {jsonb, Data} = element(7, Row),
                        Actor2#{data => nklib_json:decode(Data)};
                    true ->
                        {jsonb, Data} = element(6, Row),
                        Actor2#{data => nklib_json:decode(Data)};
                    false ->
                        Actor2
                end,
                Actor3
        end,
        Rows),
    {ok, Actors, Meta2}.


%% @private
pgsql_delete([{{delete, Total}, [], _}], Meta) ->
    {ok, Total, Meta};


pgsql_delete([{{select, _}, [{Total}], _}], Meta) ->
    {ok, Total, Meta}.


%% @private
pgsql_activate([{{select, 0}, [], _OpMeta}], Meta) ->
    {ok, [], #{meta=>Meta}};

pgsql_activate([{{select, Size}, Rows, _OpMeta}], _Meta) ->
    ActorIds = [
        #actor_id{
            uid = UID,
            namespace = Namespace,
            group = Group,
            resource = Res,
            name = Name
        }
        || {UID, Namespace, Group, Res, Name, _Last} <- Rows],
    [{_, _, _, _, _, Last}|_] = lists:reverse(Rows),
    {ok, ActorIds, #{last_time=>Last, size=>Size}}.


%% @private
pgsql_labels(Result, Meta) ->
    % lager:error("NKLOG META ~p", [_Meta]),
    #{nkactor_params:=_Params, pgsql:=#{time:=Time}} = Meta,
    {Rows, Meta2} = case Result of
        [{{select, Size}, Rows0, _OpMeta}] ->
            {Rows0, #{size=>Size, time=>Time}};
        [{{select, 1}, [{Total}], _}, {{select, Size}, Rows0, _OpMeta}] ->
            {Rows0, #{size=>Size, total=>Total, time=>Time}}
    end,
    Labels = lists:map(
        fun
            ({UID}) ->
                {UID, <<>>, <<>>};
            ({UID, Key, Value}) ->
                {UID, Key, Value}
        end,
        Rows),
    {ok, Labels, Meta2}.


%% @private
pgsql_any(List, Meta) ->
    {ok, List, #{meta=>Meta}}.


%% ===================================================================
%% Internal
%% ===================================================================


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).

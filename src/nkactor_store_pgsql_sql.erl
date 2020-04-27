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


%% @doc SQL utilities for stores to use
-module(nkactor_store_pgsql_sql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([select/2, filters/2, sort/2, field_name/2]).
-import(nkactor_store_pgsql, [quote/1, filter_path/2]).

-include_lib("nkactor/include/nkactor.hrl").


%% ===================================================================
%% Select
%% ===================================================================


%% @private
select(#{only_uid:=true}, actors) ->
    <<"SELECT uid FROM actors">>;

select(Params, actors) ->
    [
        <<"SELECT uid,namespace,\"group\",resource,name">>,
        case maps:get(get_metadata, Params, false) of
            true ->
                <<",metadata">>;
            false ->
                []
            end,
        case maps:get(get_data, Params, false) of
            true ->
                <<",data">>;
            false ->
                []
        end,
        <<" FROM actors">>
    ];

select(#{only_uid:=true}, labels) ->
    <<"SELECT uid FROM labels">>;

select(_Params, labels) ->
    <<"SELECT uid,label_key,label_value FROM labels">>.



%% ===================================================================
%% Filters
%% ===================================================================


%% @private
filters(#{namespace:=Namespace}=Params, Table) ->
    Flavor = maps:get(flavor, Params, #{}),
    Filters = maps:get(filter, Params, #{}),
    AndFilters1 = expand_filter(maps:get('and', Filters, []), []),
    AndFilters2 = make_filter(AndFilters1, Table, Flavor, []),
    OrFilters1 = expand_filter(maps:get('or', Filters, []), []),
    OrFilters2 = make_filter(OrFilters1, Table, Flavor, []),
    OrFilters3 = nklib_util:bjoin(OrFilters2, <<" OR ">>),
    OrFilters4 = case OrFilters3 of
        <<>> ->
            [];
        _ ->
            [<<$(, OrFilters3/binary, $)>>]
    end,
    NotFilters1 = expand_filter(maps:get('not', Filters, []), []),
    NotFilters2 = make_filter(NotFilters1, Table, Flavor, []),
    NotFilters3 = case NotFilters2 of
        <<>> ->
            [];
        _ ->
            [<<"(NOT ", F/binary, ")">> || F <- NotFilters2]
    end,
    Deep = maps:get(deep, Params, false),
    PathFilter = list_to_binary(filter_path(Namespace, Deep)),
    FilterList = [PathFilter | AndFilters2 ++ OrFilters4 ++ NotFilters3],
    Where = nklib_util:bjoin(FilterList, <<" AND ">>),
    [<<" WHERE ">>, Where].


%% @private
expand_filter([], Acc) ->
    Acc;

expand_filter([#{field:=Field, value:=Value}=Term|Rest], Acc) ->
    Op = maps:get(op, Term, eq),
    Type = maps:get(type, Term, string),
    Value2 = case Type of
        _ when Op==exists ->
            to_boolean(Value);
        string when Op==values, is_list(Value) ->
            [to_bin(V) || V <- Value];
        array when Op==values, is_list(Value) ->
            [to_bin(V) || V <- Value];
        string ->
            to_bin(Value);
        integer when Op==values, is_list(Value) ->
            [to_integer(V) || V <- Value];
        string_null ->
            case to_bin(Value) of
                <<"null">> -> null;
                Bin -> Bin
            end;
        integer ->
            to_integer(Value);
        boolean when Op==values, is_list(Value) ->
            [to_boolean(V) || V <- Value];
        boolean ->
            to_boolean(Value);
        object ->
            Value;
        array ->
            Value
    end,
    expand_filter(Rest, [{Field, Op, Value2, Type}|Acc]).



%% @private
make_filter([], _Table, _Flavor, Acc) ->
    Acc;

make_filter([{<<"group+resource">>, eq, Val, string} | Rest], actors, Flavor, Acc) ->
    [Group, Type] = binary:split(Val, <<"+">>),
    Filter = <<"(\"group\"='", Group/binary, "' AND resource='", Type/binary, "')">>,
    make_filter(Rest, actors, Flavor, [Filter | Acc]);

make_filter([{<<"metadata.fts.", Field/binary>>, Op, Val, string} | Rest], actors, Flavor, Acc) ->
    Word = nkactor_lib:fts_normalize_word(Val),
    Filter = case {Field, Op} of
        {<<"*">>, eq} ->
            % We search for an specific word in all fields
            % For example fieldX||valueY, found by LIKE '%||valueY %'
            % (final space makes sure word has finished)
            <<"(fts_words LIKE '%||", Word/binary, " %')">>;
        {<<"*">>, prefix} ->
            % We search for a prefix word in all fields
            % For example fieldX||valueYXX, found by LIKE '%||valueY%'
            <<"(fts_words LIKE '%||", Word/binary, "%')">>;
        {_, eq} ->
            % We search for an specific word in an specific fields
            % For example fieldX:valueYXX, found by LIKE '% FieldX:valueY %'
            <<"(fts_words LIKE '% ", Field/binary, "||", Word/binary, " %')">>;
        {_, prefix} ->
            % We search for a prefix word in an specific fields
            % For example fieldX:valueYXX, found by LIKE '% FieldX:valueY%'
            <<"(fts_words LIKE '% ", Field/binary, "||", Word/binary, "%')">>;
        _ ->
            <<"(TRUE = FALSE)">>
    end,
    make_filter(Rest, actors, Flavor, [Filter | Acc]);

make_filter([{<<"metadata.is_enabled">>, eq, true, boolean}|Rest], actors, Flavor, Acc) ->
    Filter = <<"(NOT metadata @> '{\"is_enabled\":false}')">>,
    make_filter(Rest, actors, Flavor, [Filter|Acc]);

make_filter([{<<"metadata.is_enabled">>, eq, false, boolean}|Rest], actors, Flavor, Acc) ->
    Filter = <<"(metadata @> '{\"is_enabled\":false}')">>,
    make_filter(Rest, actors, Flavor, [Filter|Acc]);

make_filter([{<<"data.", Field/binary>>, eq, Value, Type}|Rest], actors, Flavor, Acc) ->
    Json = field_value(Field, Type, Value),
    Filter = [<<"(data @> '">>, Json, <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"data.", Field/binary>>, values, Values, Type}|Rest], actors, Flavor, Acc) ->
    Filters1 = lists:foldl(
        fun(V, A) ->
            Json = field_value(Field, Type, V),
            [list_to_binary([<<"(data @> '">>, Json, <<"')">>]) | A]
        end,
        [],
        Values),
    Filters2 = nklib_util:bjoin(Filters1, <<" OR ">>),
    make_filter(Rest, actors, Flavor, [Filters2 | Acc]);

make_filter([{<<"metadata.", Field/binary>>, eq, Value, Type}|Rest], actors, Flavor, Acc) ->
    Json = field_value(Field, Type, Value),
    Filter = [<<"(metadata @> '">>, Json, <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"data.", Field/binary>>, ne, Value, Type}|Rest], actors, Flavor, Acc) ->
    Json = field_value(Field, Type, Value),
    Filter = [<<"(NOT data @> '">>, Json, <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"metadata.", Field/binary>>, ne, Value, Type}|Rest], actors, Flavor, Acc) ->
    Json = field_value(Field, Type, Value),
    Filter = [<<"(NOT metadata @> '">>, Json, <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{Field, _Op, _Val, object} | Rest], actors, Flavor, Acc) ->
    lager:warning("using invalid object operator at ~p: ~p", [?MODULE, Field]),
    make_filter(Rest, actors, Flavor, Acc);

make_filter([{Field, exists, Bool, _}|Rest], actors, Flavor, Acc)
    when Field==<<"uid">>; Field==<<"namespace">>; Field==<<"group">>;
    Field==<<"resource">>; Field==<<"path">>; Field==<<"hash">>; Field==<<"last_update">>;
    Field==<<"expires">>; Field==<<"fts_word">> ->
    Acc2 = case Bool of
        true ->
            Acc;
        false ->
            % Force no records
            [<<"(TRUE = FALSE)">>|Acc]
    end,
    make_filter(Rest, actors, Flavor, Acc2);


% NOT INDEXED for data, metadata using CR
make_filter([{Field, exists, Bool, Value}|Rest], actors, Flavor, Acc) ->
    Not = case Bool of true -> <<"NOT">>; false -> <<>> end,
    Field2 = field_name(Field, Value),
    Filter = [<<"(">>, Field2, <<" IS ">>, Not, <<" NULL)">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{Field, prefix, Val, string}|Rest], actors, Flavor, Acc) ->
    Field2 = field_name(Field, string),
    Filter = [$(, Field2, <<" LIKE ">>, quote(<<Val/binary, $%>>), $)],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter)|Acc]);

make_filter([{Field, values, ValList, Type}|Rest], actors, Flavor, Acc) when is_list(ValList) ->
    Values = nklib_util:bjoin([quote(Val) || Val <- ValList], $,),
    Field2 = field_name(Field, Type),
    Filter = [$(, Field2, <<" IN (">>, Values, <<"))">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter)|Acc]);

make_filter([{Field, values, Value, Type}|Rest], actors, Flavor, Acc) ->
    make_filter([{Field, values, [Value], Type}|Rest], actors, Flavor, Acc);

make_filter([{Field, Op, Val, Type} | Rest], actors, Flavor, Acc) ->
    Field2 = field_name(Field, Type),
    Filter = [$(, get_op(Field2, Op, Val), $)],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

% SPECIAL Label table!
make_filter([{<<"label:", Label/binary>>, Op, Value, _}|Rest], labels, Flavor, Acc)
        when Op == first; Op == top; Op == last;  Op == bottom ->
    Op2 = case Op of
        first -> <<" >= ">>;
        top -> <<" > ">>;
        last -> <<" <= ">>;
        bottom -> <<" < ">>
    end,
    Filter1 = [<<"(label_key ", Op2/binary>>, quote(Label), <<")">>],
    Filter2 = case Value of
        <<>> ->
            Filter1;
        _ ->
            [Filter1, [<<" AND (label_value ", Op2/binary>>, quote(Value), <<")">>]]
    end,
    make_filter(Rest, labels, Flavor, [list_to_binary(Filter2) | Acc]);

make_filter([{<<"label:", Label/binary>>, exists, Bool, _}|Rest], labels, Flavor, Acc) ->
    Not = case Bool of true -> []; false -> <<"NOT ">> end,
    Filter = [<<"(">>, Not, <<"label_key = ">>, quote(Label), <<")">>],
    make_filter(Rest, labels, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"label:", Label/binary>>, Op, Value, _}|Rest], labels, Flavor, Acc) ->
    Filter = [
        <<"(label_key = ">>, quote(Label), <<") AND ">>,
        <<"(">>, get_op(<<"label_value">>, Op, Value), <<")">>
    ],
    make_filter(Rest, labels, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"metadata.labels.", Label/binary>>, Op, Bool, Type}|Rest], labels, Flavor, Acc) ->
    make_filter([{<<"label:", Label/binary>>, Op, Bool, Type}|Rest], labels, Flavor, Acc).



%% @private
get_op(Field, eq, Value) -> [Field, << "=" >>, quote(Value)];
get_op(Field, ne, Value) -> [Field, <<" <> ">>, quote(Value)];
get_op(Field, lt, Value) -> [Field, <<" < ">>, quote(Value)];
get_op(Field, lte, Value) -> [Field, <<" <= ">>, quote(Value)];
get_op(Field, gt, Value) -> [Field, <<" > ">>, quote(Value)];
get_op(Field, gte, Value) -> [Field, <<" >= ">>, quote(Value)];
get_op(_Field, exists, _Value) -> [<<"TRUE">>];
get_op(Field, range, Value) ->
    case binary:split(Value, <<"|">>) of
        [V1, V2] ->
            [
                $(,
                case V1 of
                    <<"!", V1b/binary>> -> [Field, <<" > ">>, quote(V1b)];
                    _ -> [Field, <<" >= ">>, quote(V1)]
                end,
                " ) AND ( ",
                case V2 of
                    <<"!", V2b/binary>> -> [Field, <<" < ">>, quote(V2b)];
                    _ -> [Field, <<" <= ">>, quote(V2)]
                end,
                $)

            ];
        _ ->
            get_op(Field, eq, Value)
    end.



%% @private
field_db_name(<<"uid">>) -> <<"uid">>;
field_db_name(<<"namespace">>) -> <<"namespace">>;
field_db_name(<<"group">>) -> <<"\"group\"">>;
field_db_name(<<"resource">>) -> <<"resource">>;
field_db_name(<<"name">>) -> <<"name">>;
field_db_name(<<"hash">>) -> <<"hash">>;
field_db_name(<<"path">>) -> <<"path">>;
field_db_name(<<"last_update">>) -> <<"last_update">>;
field_db_name(<<"expires">>) -> <<"expires">>;
field_db_name(<<"fts_word">>) -> <<"fts_word">>;
%field_db_name(<<"data.", _/binary>>=Field) -> Field;
field_db_name(<<"metadata.hash">>) -> <<"hash">>;
field_db_name(<<"metadata.update_time">>) -> <<"last_update">>;
% Any other metadata is kept
%field_db_name(<<"metadata.", _/binary>>=Field) -> Field;
% Any other field should be inside data in this implementation
%field_db_name(Field) -> <<"data.", Field/binary>>.
field_db_name(Field) -> Field.



%% ===================================================================
%% Sort
%% ===================================================================


%% @private
sort(Params, Table) ->
    Flavor = maps:get(flavor, Params, #{}),
    Sort = expand_sort(maps:get(sort, Params, []), []),
    make_sort(Sort, Table, Flavor, []).


%% @private
expand_sort([], Acc) ->
    lists:reverse(Acc);

expand_sort([#{field:=Field}=Term|Rest], Acc) ->
    case Field of
        <<"group+resource">> ->
            % Special field used in namespaces
            expand_sort([Term#{field:=<<"group">>}, Term#{field:=<<"resource">>}|Rest], Acc);
        _ ->
            Order = maps:get(order, Term, asc),
            Type = maps:get(type, Term, string),
            expand_sort(Rest, [{Order, Field, Type}|Acc])
    end.


%% @private
make_sort([], _Table, _Flavor, []) ->
    <<>>;

make_sort([], _Table, _Flavor, Acc) ->
    [<<" ORDER BY ">>, nklib_util:bjoin(lists:reverse(Acc), $,)];

make_sort([{Order, Field, Type}|Rest], actors, Flavor, Acc) ->
    Item = [
        field_name(Field, Type),
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end
    ],
    make_sort(Rest, actors, Flavor, [list_to_binary(Item)|Acc]);

make_sort([{Order, <<"label_key">>, _Type}|Rest], labels, Flavor, Acc) ->
    Item = [
        <<"label_key">>,
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end
    ],
    make_sort(Rest, labels, Flavor, [list_to_binary(Item)|Acc]);

make_sort([{Order, <<"label_value">>, _Type}|Rest], labels, Flavor, Acc) ->
    Item = [
        <<"label_value">>,
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end
    ],
    make_sort(Rest, labels, Flavor, [list_to_binary(Item)|Acc]);

make_sort([{Order, <<"label:", _/binary>>, _Type}|Rest], labels, Flavor, Acc) ->
    Item = [
        <<"label_key">>,
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end,
        <<", label_value">>,
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end
    ],
    make_sort(Rest, labels, Flavor, [list_to_binary(Item)|Acc]);

make_sort([{Order, <<"metadata.labels.", Label/binary>>, Type}|Rest], labels, Flavor, Acc) ->
    make_sort([{Order, <<"label:", Label/binary>>, Type}|Rest], labels, Flavor, Acc).



%% ===================================================================
%% Utilities
%% ===================================================================


%% @private
%% Extracts a field inside a JSON, it and casts it to json, string, integer o boolean
field_name(Field, Type) ->
    Field2 = field_db_name(Field),
    list_to_binary(field_name(Field2, Type, [], [])).


%% @private
% We now that anything from there is not nested any more
field_name(Field, Type, [<<"links">>, <<"metadata">>], Acc) ->
    finish_field_name(Type, Field, Acc);

% We now that anything from there is not nested any more
field_name(Field, Type, [<<"labels">>, <<"metadata">>], Acc) ->
    finish_field_name(Type, Field, Acc);

field_name(Field, Type, Heads, Acc) ->
    case binary:split(Field, <<".">>) of
        [Last] when Acc==[] ->
            [Last];
        [Last] ->
            finish_field_name(Type, Last, Acc);
        [Base, Rest] when Acc==[] andalso (Base == <<"metadata">> orelse Base == <<"data">>) ->
            field_name(Rest, Type, [Base|Heads], [Base, <<"->">>]);
        [Base, Rest] ->
            field_name(Rest, Type, [Base|Heads], Acc++[$', Base, $', <<"->">>])
    end.

finish_field_name(Type, Last, Acc) ->
    case Type of
        json ->
            Acc++[$', Last, $'];
        string ->
            Acc++[$>, $', Last, $'];    % '>' finishes ->>
        string_null ->
            Acc++[$>, $', Last, $'];    % '>' finishes ->>
        array ->
            Acc++[$>, $', Last, $'];    % '>' finishes ->>
        integer ->
            [$(|Acc] ++ [$>, $', Last, $', <<")::INTEGER">>];
        boolean ->
            [$(|Acc] ++ [$>, $', Last, $', <<")::BOOLEAN">>]
    end.


%% @private Generates a JSON based on a field
%% make_json_spec(<<"a.b.c">>) = {"a":{"b":"c"}}
field_value(Field, Type, Value) ->
    List = binary:split(Field, <<".">>, [global]),
    Value2 = case Type of
        array -> [Value];
        _ -> Value
    end,
    Map = field_value(List++[Value2]),
    nklib_json:encode(Map).


%% @private
field_value([]) -> #{};
field_value([Last]) -> Last;
field_value([Field|Rest]) -> #{Field => field_value(Rest)}.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).


%% @private
to_integer(Term) when is_integer(Term) ->
    Term;
to_integer(Term) ->
    case nklib_util:to_integer(Term) of
        error -> 0;
        Integer -> Integer
    end.

%% @private
to_boolean(Term) when is_boolean(Term) ->
    Term;
to_boolean(Term) ->
    case nklib_util:to_boolean(Term) of
        error -> false;
        Boolean -> Boolean
    end.

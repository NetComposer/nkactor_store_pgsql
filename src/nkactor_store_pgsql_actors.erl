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

-module(nkactor_store_pgsql_actors).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([find/3, read/3, create/3, update/3, delete/3, delete_multi/3]).
-export([get_links/3, get_linked/3]).
-import(nkactor_store_pgsql, [query/2, query/3, quote/1]).

-define(MAX_CASCADE_DELETE, 10000).
-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR PGSQL "++Txt, Args)).


-include_lib("nkactor/include/nkactor.hrl").
-include("nkactor_store_pgsql.hrl").


%% ===================================================================
%% Types
%% ===================================================================


%% ===================================================================
%% API
%% ===================================================================

%% @doc
find(SrvId, #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace}=ActorId, _Opts)
        when is_binary(Group), is_binary(Res), is_binary(Name), is_binary(Namespace) ->
    Query = [
        <<"SELECT uid FROM actors">>,
        <<" WHERE namespace=">>, quote(Namespace),
        <<" AND \"group\"=">>, quote(Group),
        <<" AND resource=">>, quote(Res),
        <<" AND name=">>, quote(Name), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, [[{UID2}]], QueryMeta} ->
            {ok, ActorId#actor_id{uid=UID2, pid=undefined}, QueryMeta};
        {ok, [[]], _} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end;

find(SrvId, #actor_id{uid=UID}, _Opts) when is_binary(UID) ->
    Query = [
        <<"SELECT namespace,\"group\",resource,name FROM actors">>,
        <<" WHERE uid=">>, quote(UID), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, [[{Namespace2, Group2, Res2, Name2}]], QueryMeta} ->
            ActorId2 = #actor_id{
                uid = UID,
                group = Group2,
                resource = Res2,
                name = Name2,
                namespace = Namespace2
            },
            {ok, ActorId2, QueryMeta};
        {ok, [[]], _} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end;

find(SrvId, #actor_id{resource=Res, name=Name, namespace=Namespace}=ActorId, _Opts)
        when is_binary(Res), is_binary(Name), is_binary(Namespace) ->
    Query = [
        <<"SELECT uid,\"group\" FROM actors">>,
        <<" WHERE namespace=">>, quote(Namespace),
        <<" AND resource=">>, quote(Res),
        <<" AND name=">>, quote(Name), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, [[{UID, Group}]], QueryMeta} ->
            {ok, ActorId#actor_id{uid=UID, group=Group}, QueryMeta};
        {ok, _, _} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end;

find(SrvId, #actor_id{name=Name, namespace=Namespace}=ActorId, _Opts)
        when is_binary(Name), is_binary(Namespace) ->
    Query = [
        <<"SELECT uid,\"group\",resource FROM actors">>,
        <<" WHERE namespace=">>, quote(Namespace),
        <<" AND name=">>, quote(Name), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, [[{UID, Group, Res}]], QueryMeta} ->
            {ok, ActorId#actor_id{uid=UID, group=Group, resource=Res}, QueryMeta};
        {ok, _, _} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
read(SrvId, #actor_id{namespace=Namespace, group=Group, resource=Res, name=Name}, _Opts)
        when is_binary(Group), is_binary(Res), is_binary(Name), is_binary(Namespace) ->
    Query = [
            <<"SELECT uid,metadata,data FROM actors ">>,
            <<" WHERE namespace=">>, quote(Namespace),
            <<" AND \"group\"=">>, quote(Group),
            <<" AND resource=">>, quote(Res),
            <<" AND name=">>, quote(Name), <<";">>
        ],
        case query(SrvId, Query) of
            {ok, [[Fields]], QueryMeta} ->
                {UID, {jsonb, Meta}, {jsonb, Data}} = Fields,
                Actor = #{
                    group => Group,
                    resource => Res,
                    name => Name,
                    namespace => Namespace,
                    uid => UID,
                    data => nklib_json:decode(Data),
                    metadata => nklib_json:decode(Meta)
                },
                {ok, Actor, QueryMeta};
            {ok, [[]], _QueryMeta} ->
                {error, actor_not_found};
            {error, Error} ->
                {error, Error}
        end;

read(SrvId, #actor_id{uid=UID}, _Opts) when is_binary(UID) ->
    Query = [
        <<"SELECT namespace,\"group\",resource,name,metadata,data FROM actors ">>,
        <<" WHERE uid=">>, quote(UID), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, [[Fields]], QueryMeta} ->
            {Namespace, Group, Res, Name, {jsonb, Meta}, {jsonb, Data}} = Fields,
            Actor = #{
                group => Group,
                resource => Res,
                name => Name,
                namespace => Namespace,
                uid => UID,
                data => nklib_json:decode(Data),
                metadata => nklib_json:decode(Meta)
            },
            {ok, Actor, QueryMeta};
        {ok, [[]], _QueryMeta} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end;

read(SrvId, #actor_id{namespace=Namespace, resource=Res, name=Name}, _Opts)
        when is_binary(Res), is_binary(Name), is_binary(Namespace) ->
    Query = [
        <<"SELECT uid,\"group\",metadata,data FROM actors ">>,
        <<" WHERE namespace=">>, quote(Namespace),
        <<" AND resource=">>, quote(Res),
        <<" AND name=">>, quote(Name), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, [[Fields]], QueryMeta} ->
            {UID, Group, {jsonb, Meta}, {jsonb, Data}} = Fields,
            Actor = #{
                group => Group,
                resource => Res,
                name => Name,
                namespace => Namespace,
                uid => UID,
                data => nklib_json:decode(Data),
                metadata => nklib_json:decode(Meta)
            },
            {ok, Actor, QueryMeta};
        {ok, _, _QueryMeta} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end;

read(SrvId, #actor_id{namespace=Namespace, name=Name}, _Opts)
        when is_binary(Name), is_binary(Namespace) ->
    Query = [
        <<"SELECT uid,\"group\",resource,metadata,data FROM actors ">>,
        <<" WHERE namespace=">>, quote(Namespace),
        <<" AND name=">>, quote(Name), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, [[Fields]], QueryMeta} ->
            {UID, Group, Res, {jsonb, Meta}, {jsonb, Data}} = Fields,
            Actor = #{
                group => Group,
                resource => Res,
                name => Name,
                namespace => Namespace,
                uid => UID,
                data => nklib_json:decode(Data),
                metadata => nklib_json:decode(Meta)
            },
            {ok, Actor, QueryMeta};
        {ok, _, _QueryMeta} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end.


-record(save_fields, {
    names = [],
    uids = [],
    actors = [],
    labels = [],
    links = [],
    fts = []
}).


%% @doc
create(SrvId, Actor, _Opts) ->
    save(SrvId, create, Actor).


%% @doc
update(SrvId, Actor, _Opts) ->
    save(SrvId, update, Actor).



%% @doc Called from actor_srv_save callback
%% Links to invalid objects will not be allowed (foreign key)
save(SrvId, Mode, Actor) when is_map(Actor) ->
    save(SrvId, Mode, [Actor]);

save(SrvId, Mode, Actors) ->
    Flavour = nkserver:get_cached_config(SrvId, nkpgsql, flavour),
    Fields = populate_fields(Actors, Mode, #save_fields{}),
    #save_fields{
        names = FieldNames,
        uids = UIDs,
        actors = ActorsFields,
        labels = LabelFields,
        links = LinkFields,
        fts = FtsFields
    } = Fields,
    ActorsQuery = case Mode of
        create ->
            FieldNames2 = nklib_util:bjoin(FieldNames),
            [
                <<"INSERT INTO actors (">>, FieldNames2, <<") VALUES ">>,
                nklib_util:bjoin(
                    [
                        list_to_binary([<<"(">>, nklib_util:bjoin(ActorFields), <<")">>])
                        || ActorFields <- ActorsFields
                    ]),
                return_nothing(Flavour)
            ];
        update ->
            lists:map(
                fun(ActorFields) ->
                    Values = lists:zip(FieldNames, ActorFields),
                    [
                        <<"UPDATE actors SET ">>,
                        nklib_util:bjoin([
                            list_to_binary([Field, <<"=">>, Value])
                            || {Field, Value} <- Values, Field /= <<"uid">>
                        ]),
                        <<" WHERE uid=">>, hd(ActorFields), return_nothing(Flavour)
                    ]
                end,
                ActorsFields)
    end,
    LabelsQuery = [
        case Mode of
            create ->
                [];
            update ->
                [<<"DELETE FROM labels WHERE uid IN ">>, UIDs, return_nothing(Flavour)]
        end,
        case LabelFields of
            [] ->
                [];
            _ ->
                [
                    <<"INSERT INTO labels (uid,label_key,label_value,path) VALUES ">>,
                    nklib_util:bjoin(LabelFields), return_nothing(Flavour)
                ]
        end
    ],
    LinksQuery = [
        case Mode of
            create ->
                [];
            update ->
                [<<"DELETE FROM links WHERE uid IN ">>, UIDs, return_nothing(Flavour)]
        end,
        case LinkFields of
            [] ->
                [];
            _ ->
                [
                    <<"INSERT INTO links (uid,link_target,link_type,path) VALUES ">>,
                    nklib_util:bjoin(LinkFields), return_nothing(Flavour)
                ]
        end
    ],
    FtsQuery = [
        case Mode of
            create ->
                [];
            update ->
                [<<"DELETE FROM fts WHERE uid IN ">>, UIDs, return_nothing(Flavour)]
        end,
        case FtsFields of
            [] ->
                [];
            _ ->
                [
                    <<"INSERT INTO fts (uid,fts_word,fts_field,path) VALUES ">>,
                    nklib_util:bjoin(FtsFields), return_nothing(Flavour)
                ]
        end
    ],

    Query = [
            <<"BEGIN;">>,
            ActorsQuery,
            LabelsQuery,
            LinksQuery,
            FtsQuery,
            <<"COMMIT;">>
    ],
    case query(SrvId, Query, #{auto_rollback=>true}) of
        {ok, _, SaveMeta} ->
            {ok, SaveMeta};
        {error, foreign_key_violation} ->
            ?LLOG(warning, "Foreign Key Violantion: ~s", [list_to_binary(Query)]),
            {error, linked_actor_unknown};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
delete(SrvId, #actor_id{uid=UID}=ActorId, _Opts) when is_binary(UID) ->
    Debug = nkserver:get_cached_config(SrvId, nkactor_store_pgsql, debug),
    Flavour = nkserver:get_cached_config(SrvId, nkpgsql, flavour),
    QUID = quote(UID),
    % Labels, FTS and links will be deleted automatically
    % If other actor has a link to us, it will return 'actor_has_linked_actors'
    Query = [<<"DELETE FROM actors WHERE uid=">>, QUID, return_nothing(Flavour)],
    QueryOpts = #{
        pgsql_debug => Debug
    },
    case query(SrvId, Query, QueryOpts) of
        {ok, _, Meta} ->
            % In case the actor is still active, terminate it
            nkactor_srv:raw_stop(ActorId, backend_deleted),
            {ok, Meta};
        {error, foreign_key_violation} ->
            {error, actor_has_linked_actors};
        {error, Error} ->
            {error, Error}
    end;

delete(SrvId, ActorId, Opts)  ->
    case find(SrvId, ActorId, Opts) of
        {ok, #actor_id{uid=UID}=ActorId2, _Meta} when is_binary(UID) ->
            delete(SrvId, ActorId2, Opts);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
%% Deletes a set ob objects
%% In a first pass, it will delete actors without anyone linking to it
%% Then it will do new passes, until all are deleted or no new actor can be deleted
%% (because are other actors linking to it)

delete_multi(SrvId, Ids, Opts) ->
    do_delete_multi(SrvId, Ids, Opts, 0).


%% @private
do_delete_multi(SrvId, Ids, Opts, Count) ->
    case do_delete_multi(SrvId, Ids, Opts, [], [], Count) of
        {ok, [], [], Count2} ->
            {ok, #{deleted => Count2}};
        {ok, Deleted, NotDeleted, Count2} when length(Deleted) > 0 ->
            do_delete_multi(SrvId, NotDeleted, Opts, Count2);
        {ok, _, [NotDeleted|_], _Count2} ->
            {error, {actor_not_deleted, NotDeleted}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_delete_multi(_SrvId, [], _Opts, Deleted, NotDeleted, Count) ->
    {ok, Deleted, NotDeleted, Count};

do_delete_multi(SrvId, [Id|Rest], Opts, Deleted, NotDeleted, Count) ->
    case delete(SrvId, Id, Opts) of
        {ok, _} ->
            do_delete_multi(SrvId, Rest, Opts, [Id|Deleted], NotDeleted, Count+1);
        {error, actor_has_linked_actors} ->
            do_delete_multi(SrvId, Rest, Opts, Deleted, [Id|NotDeleted], Count);
        {error, Error} ->
            {error, Error}
    end.



%% @private
return_nothing(cockroachdb) -> <<" RETURNING NOTHING; ">>;
return_nothing(_) -> <<"; ">>.



%% @private
populate_fields([], _Op, #save_fields{uids=UIDs}=SaveFields) ->
    UIDs2 = list_to_binary([<<"(">>, nklib_util:bjoin(UIDs, $,), <<")">>]),
    SaveFields#save_fields{uids=UIDs2};

populate_fields([Actor|Rest], Op, SaveFields) ->
    #save_fields{
        uids = UIDs,
        actors = Actors,
        labels = Labels,
        links = Links,
        fts = Fts
    } = SaveFields,
    #{
        uid := UID,
        namespace := Namespace,
        group := Group,
        resource := Res,
        name := Name,
        data := Data,
        metadata := Meta
    } = Actor,
    true = is_binary(UID) andalso UID /= <<>>,
    Path = nkactor_lib:make_rev_path(Namespace),
    QUID = quote(UID),
    QPath = quote(Path),
    Hash = maps:get(hash, Meta, <<>>),
    Updated = maps:get(update_time, Meta),
    Activate = case Meta of
        #{auto_activate:=true} ->
            <<"1">>;
        _ ->
            case maps:get(activate_time, Meta, <<>>) of
                <<>> ->
                    null;
                Exp1 ->
                    Exp1
        end
    end,
    FtsWords1 = maps:fold(
        fun(Key, Text, Acc) ->
            Acc#{Key => nkactor_lib:fts_normalize_multi(Text)}
        end,
        #{},
        maps:get(fts, Meta, #{})),
    FtsWords2 = maps:fold(
        fun(Key, Values, Acc1) ->
            lists:foldl(
                fun(Value, Acc2) ->
                    [<<" ">>, to_bin(Key), "||" , to_bin(Value) | Acc2]
                end,
                Acc1,
                Values)
        end,
        [],
        FtsWords1),
    Actor2 = [
        quote(Data),
        quote(Meta),
        quote(Hash),
        quote(Updated),
        quote(Activate),
        quote(list_to_binary([FtsWords2, <<" ">>]))
    ],
    Actor3 = case Op of
        create ->
            [
                QUID,
                quote(Group),
                quote(Res),
                quote(Name),
                quote(Namespace),
                QPath
                | Actor2
            ];
        update ->
            [
                QUID,
                quote(Name),
                quote(Namespace),
                QPath
                |
                Actor2
            ]
    end,

    %Actors2 = [list_to_binary([<<"(">>, ActorFields, <<")">>]) | Actors],
    Labels2 = maps:fold(
        fun(Key, Val, Acc) ->
            L = list_to_binary([
                <<"(">>, QUID, $,, quote(Key), $,, quote(to_bin(Val)), $,, QPath, <<")">>
            ]),
            [L|Acc]
        end,
        Labels,
        maps:get(labels, Meta, #{})),
    Links2 = maps:fold(
        fun(UID2, LinkType, Acc) ->
            L = list_to_binary([
                <<"(">>, QUID, $,, quote(UID2), $,, quote(LinkType), $,, QPath, <<")">>
            ]),
            [L|Acc]
        end,
        Links,
        maps:get(links, Meta, #{})),
    Fts2 = maps:fold(
        fun(Field, WordList, Acc1) ->
            lists:foldl(
                fun(Word, Acc2) ->
                    L = list_to_binary([
                        <<"(">>, QUID, $,, quote(Word), $,, quote(Field), $,, QPath, <<")">>
                    ]),
                    [L|Acc2]
                end,
                Acc1,
                WordList)
        end,
        Fts,
        FtsWords1),
    FieldNames1 = [
        <<"data">>, <<"metadata">>, <<"hash">>, <<"last_update">>,
        <<"activate">>, <<"fts_words">>
    ],
    FieldNames2 = case Op of
        create ->
            [
                <<"uid">>, <<"\"group\"">>, <<"resource">>,
                <<"name">>, <<"namespace">>, <<"path">> | FieldNames1
            ];
        update ->
            [
                <<"uid">>,
                <<"name">>, <<"namespace">>, <<"path">> | FieldNames1
            ]
    end,
    SaveFields2 = SaveFields#save_fields{
        names = FieldNames2,
        uids = [QUID|UIDs],
        actors = [Actor3|Actors],
        labels = Labels2,
        links = Links2,
        fts = Fts2
    },
    populate_fields(Rest, Op, SaveFields2).







%%
%%%% @doc
%%%% Option 'cascade' to delete all linked
%%delete(SrvId, UID, Opts) when is_binary(UID) ->
%%    delete(SrvId, [UID], Opts);
%%
%%delete(SrvId, UIDs, Opts) ->
%%    lager:error("NKLOG DEL1"),
%%    Debug = nkserver:get_cached_config(SrvId, nkactor_store_pgsql, debug),
%%    QueryMeta = #{pgsql_debug=>Debug},
%%    QueryFun = fun(Pid) ->
%%        lager:error("NKLOG F1"),
%%        {ok, _, _} = nkpgsql:do_query(Pid, <<"BEGIN;">>, QueryMeta),
%%        lager:error("NKLOG F2"),
%%        {ActorIds, DelQ} = case Opts of
%%            #{cascade:=true} ->
%%                NestedUIDs = delete_find_nested(Pid, UIDs, sets:new()),
%%                ?LLOG(notice, "DELETE on CASCADE: ~p", [NestedUIDs]),
%%                delete_actors(SrvId, NestedUIDs, false, Pid, QueryMeta, [], []);
%%            _ ->
%%                delete_actors(SrvId, UIDs, true, Pid, QueryMeta, [], [])
%%        end,
%%        lager:error("NKLOG F3"),
%%        {ok, _, _} = nkpgsql:do_query(Pid, DelQ, QueryMeta),
%%        lager:error("NKLOG F4"),
%%        case nkpgsql:do_query(Pid, <<"COMMIT;">>, QueryMeta#{deleted_actor_ids=>ActorIds}) of
%%            {ok, DeletedActorIds, DeletedMeta} ->
%%                lager:error("NKLOG DEL3"),
%%                % Actors could have been reactivated after the raw_stop and before the
%%                % real deletion
%%%%                lists:foreach(
%%%%                    fun(#actor_id{uid=DUID}) ->
%%%%                        nkactor_srv:raw_stop({SrvId, DUID}, actor_deleted)
%%%%                    end,
%%%%                    DeletedActorIds),
%%                {ok, DeletedActorIds, DeletedMeta};
%%            Other ->
%%                lager:error("NKLOG DEL3"),
%%                Other
%%        end
%%    end,
%%    lager:error("NKLOG DEL5A"),
%%    case query(SrvId, QueryFun, #{}) of
%%        {ok, _, Meta1} ->
%%            lager:error("NKLOG DEL5"),
%%            {ActorIds2, Meta2} = maps:take(deleted_actor_ids, Meta1),
%%            {ok, ActorIds2, Meta2};
%%        {error, Error} ->
%%            lager:error("NKLOG DEL6"),
%%            {error, Error}
%%    end.
%%
%%
%%delete_find_nested(_Pid, [], Set) ->
%%    sets:to_list(Set);
%%
%%delete_find_nested(Pid, [UID|Rest], Set) ->
%%    case sets:is_element(UID, Set) of
%%        true ->
%%            delete_find_nested(Pid, Rest, Set);
%%        false ->
%%            Set2 = sets:add_element(UID, Set),
%%            case sets:size(Set2) > ?MAX_CASCADE_DELETE of
%%                true ->
%%                    throw(delete_too_deep);
%%                false ->
%%                    Q = [<<" SELECT uid FROM links WHERE link_target=">>, quote(UID), <<";">>],
%%                    Childs = case nkpgsql:do_query(Pid, Q, #{}) of
%%                        {ok, [[]], _} ->
%%                            [];
%%                        {ok, [List], _} ->
%%                            [U || {U} <- List]
%%                    end,
%%                    delete_find_nested(Pid, Childs++Rest, Set2)
%%            end
%%    end.
%%
%%
%%%% @private
%%delete_actors(_SrvId, [], _CheckChilds, _Pid, _QueryMeta, ActorIds, QueryAcc) ->
%%    {ActorIds, QueryAcc};
%%
%%delete_actors(SrvId, [UID|Rest], CheckChilds, Pid, QueryMeta, ActorIds, QueryAcc) ->
%%    Flavour = nkserver:get_cached_config(SrvId, nkpgsql, flavour),
%%    case nkactor_srv:raw_stop(UID, pre_delete) of
%%        ok ->
%%            ok;
%%        {error, RawError} ->
%%            ?LLOG(warning, "could not send pre_delete to ~s: ~p", [UID, RawError])
%%    end,
%%    QUID = quote(UID),
%%    case CheckChilds of
%%        true ->
%%            LinksQ = [<<"SELECT uid FROM links WHERE link_target=">>, QUID, <<";">>],
%%            case nkpgsql:do_query(Pid, LinksQ, QueryMeta) of
%%                {ok, [[]], _} ->
%%                    ok;
%%                _ ->
%%                    throw(actor_has_linked_actors)
%%            end;
%%        false ->
%%            ok
%%    end,
%%    GetQ = [
%%        <<"SELECT namespace,\"group\",resource,name FROM actors ">>,
%%        <<"WHERE uid=">>, quote(UID), <<";">>
%%    ],
%%    case nkpgsql:do_query(Pid, GetQ, QueryMeta) of
%%        {ok, [[{Namespace, Group, Res, Name}]], _} ->
%%            ActorId = #actor_id{
%%                namespace = Namespace,
%%                uid = UID,
%%                group = Group,
%%                resource = Res,
%%                name = Name
%%            },
%%            QueryAcc2 = [
%%                <<"DELETE FROM actors WHERE uid=">>, QUID, return_nothing(Flavour),
%%                <<"DELETE FROM labels WHERE uid=">>, QUID, return_nothing(Flavour),
%%                <<"DELETE FROM links WHERE uid=">>, QUID, return_nothing(Flavour),
%%                <<"DELETE FROM fts WHERE uid=">>, QUID, return_nothing(Flavour)
%%                | QueryAcc
%%            ],
%%            delete_actors(SrvId, Rest, CheckChilds, Pid, QueryMeta, [ActorId|ActorIds], QueryAcc2);
%%        {ok, [[]], _} ->
%%            throw(actor_not_found);
%%        {error, Error} ->
%%            throw(Error)
%%    end.



%% @doc Gets, for an UID, all links it has to other objects
%% Returns {Type, UID}
get_links(SrvId, UID, Type) ->
    UID2 = to_bin(UID),
    Query = [
        <<"SELECT uid2,type FROM links">>,
        <<" WHERE uid1=">>, quote(UID2),
        case Type of
            <<>> ->
                <<>>;
            _ ->
                [<<" AND type=">>, quote(Type)]
        end,
        <<";">>
    ],
    case query(SrvId, Query, #{}) of
        {ok, [List], Meta} ->
            {ok, List, Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
%% Returns [{UID, Type}]
get_linked(SrvId, UID, Type) ->
    UID2 = to_bin(UID),
    Query = [
        <<"SELECT uid1,type FROM links">>,
        <<" WHERE uid2=">>, quote(UID2),
        case Type of
            <<>> ->
                <<>>;
            _ ->
                [<<" AND type=;">>, quote(Type)]
        end,
        <<";">>
    ],
    case query(SrvId, Query, #{}) of
        {ok, [List], Meta} ->
            {ok, List, Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).

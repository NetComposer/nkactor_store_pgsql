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

%% @doc Default callbacks for plugin definitions
-module(nkactor_store_pgsql_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_config/3, plugin_cache/3, plugin_start/3]).

-include_lib("nkactor/include/nkactor.hrl").
-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Plugin Callbacks
%% ===================================================================


%% @doc
plugin_deps() ->
	[nkactor].


%% @doc
plugin_config(_SrvId, Config, #{class:=nkactor}) ->
	Syntax = #{
		pgsql_service => atom,
		debug => boolean
	},
	nkserver_util:parse_config(Config, Syntax).


%% @doc
plugin_cache(_SrvId, Config, _Service) ->
	PgsqlService = maps:get(pgsql_service, Config, undefined),
	Debug = maps:get(debug, Config, false),
	{ok, #{
		pgsql_service => PgsqlService,
		debug => Debug
	}}.


%% @doc
plugin_start(SrvId, _Config, _Service) ->
	case nkactor_store_pgsql:get_pgsql_srv(SrvId) of
		undefined ->
			ok;
		PgSrvId ->
			nkactor_store_pgsql_init:init(PgSrvId),
		?CALL_SRV(SrvId, actor_db_init, [SrvId])
	end.

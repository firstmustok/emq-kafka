%%--------------------------------------------------------------------
%% Copyright (c) 2015-2017 Feng Lee <feng@emqtt.io>.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_acl_kafka).

-include_lib("emqttd/include/emqttd.hrl").

%% ACL callbacks
-export([check_acl/2, description/0, init/1,
	 reload_acl/1]).

init(Opts) -> {ok, Opts}.

check_acl({Client, PubSub, Topic}, _Opts) ->
    io:format("ACL Demo: ~p ~p ~p~n",
	      [Client, PubSub, Topic]),
    ignore.

reload_acl(_Opts) -> ignore.

description() -> "ACL Demo Module".

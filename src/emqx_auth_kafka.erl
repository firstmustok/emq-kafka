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

-module(emqx_auth_kafka).

-include_lib("emqx/include/emqx.hrl").

%% ACL callbacks
-export([ init/1
        , check/2
        , description/0
        ]).


init(Opts) -> 
  {ok, Opts}.

check(_Credentials = #{client_id := ClientId, username := Username, password := Password}, _State) ->
    io:format("Auth Demo: clientId=~p, username=~p, password=~p~n", [ClientId, Username, Password]),
    ok.

description() -> "Auth kafka Module".

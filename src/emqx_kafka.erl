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

-module(emqx_kafka).

-include_lib("emqx/include/emqx.hrl").

-import(string,[concat/2]).
-import(lists,[nth/2]). 

-export([load/1, unload/0]).

%% Hooks functions
-export([ %%on_client_authenticate/2
        %%, on_client_check_acl/5
        on_client_connected/4
        , on_client_disconnected/3
        , on_client_subscribe/4
        , on_client_unsubscribe/4
        , on_session_created/3
        , on_session_resumed/3
        , on_session_terminated/3
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_message_publish/2
        , on_message_deliver/3
        , on_message_acked/3
        ]).

%% Called when the plugin application start
load(Env) ->
    ekaf_init([Env]),
    brod_init([Env]),

    %% emqx:hook('client.authenticate', fun ?MODULE:on_client_authenticate/2, [Env]),
    %% emqx:hook('client.check_acl', fun ?MODULE:on_client_check_acl/5, [Env]),
    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
    emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
    emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    emqx:hook('session.resumed', fun ?MODULE:on_session_resumed/3, [Env]),
    emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/3, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [Env]),
    emqx:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

%% on_client_authenticate(ClientInfo = #{clientid := ClientId, password := Password}, _Env) ->
%%     io:format("Client(~s) authenticate, Password:~p ~n", [ClientId, Password]),
%%     {stop, ClientInfo#{auth_result => success}}.
%% 
%% on_client_check_acl(#{clientid := ClientId}, PubSub, Topic, DefaultACLResult, _Env) ->
%%     io:format("Client(~s) authenticate, PubSub:~p, Topic:~p, DefaultACLResult:~p~n",
%%                 [ClientId, PubSub, Topic, DefaultACLResult]),
%%     {stop, allow}.

on_client_connected(#{clientid := ClientId, peerhost := ClientIp}, ConnAck, ConnAttrs, _Env) ->
    io:format("Client(~s) connected, connack: ~w, conn_attrs:~p~n", [ClientId, ConnAck, ConnAttrs]).

on_client_disconnected(#{clientid := ClientId}, ReasonCode, _Env) ->
    io:format("Client(~s) disconnected, reason_code: ~w~n", [ClientId, ReasonCode]).

on_client_subscribe(#{clientid := ClientId}, _Properties, RawTopicFilters, _Env) ->
    io:format("Client(~s) will subscribe: ~p~n", [ClientId, RawTopicFilters]),
    {ok, RawTopicFilters}.

on_client_unsubscribe(#{clientid := ClientId}, _Properties, RawTopicFilters, _Env) ->
    io:format("Client(~s) unsubscribe ~p~n", [ClientId, RawTopicFilters]),
    {ok, RawTopicFilters}.

on_session_created(#{clientid := ClientId}, SessAttrs, _Env) ->
    io:format("Session(~s) created: ~p~n", [ClientId, SessAttrs]).

on_session_resumed(#{clientid := ClientId}, SessAttrs, _Env) ->
    io:format("Session(~s) resumed: ~p~n", [ClientId, SessAttrs]).

on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
    io:format("Session(~s) subscribe ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
    io:format("Session(~s) unsubscribe ~s with opts: ~p~n", [ClientId, Topic, Opts]).

on_session_terminated(#{clientid := ClientId}, ReasonCode, _Env) ->
    io:format("Session(~s) terminated: ~p.", [ClientId, ReasonCode]).

%% transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #message{qos = Qos,
                        %% retain  = Retain,
                        topic   = Topic,
                        payload = Payload
						}, _Env) ->
    io:format("publish ~s~n", [emqx_message:format(Message)]),
    Str1 = <<"{\"topic\":\"">>,
    Str2 = <<"\", \"message\":[">>,
    Str3 = <<"]}">>,
    Str4 = <<Str1/binary, Topic/binary, Str2/binary, Payload/binary, Str3/binary>>,
	{ok, KafkaTopic} = application:get_env(emqx_kafka, values),
    ProduceTopic = proplists:get_value(kafka_producer_topic, KafkaTopic),
    ekaf:produce_async(ProduceTopic, Str4),	
    {ok, Message}.

on_message_deliver(#{clientid := ClientId}, Message, _Env) ->
    io:format("Deliver message to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

on_message_acked(#{clientid := ClientId}, Message, _Env) ->
    io:format("Session(~s) acked message: ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

ekaf_init(_Env) ->
    {ok, Values} = application:get_env(emqx_kafka, values),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values),
    PartitionStrategy= proplists:get_value(partition_strategy, Values),
    application:set_env(ekaf, ekaf_partition_strategy, PartitionStrategy),
    application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),
    {ok, _} = application:ensure_all_started(ekaf),
    io:format("Initialized ekaf with ~p~n", [{"localhost", 9092}]).

%% ===================================================================
%% brod_init https://github.com/klarna/brod
%% ===================================================================
brod_init(_Env) ->
    {ok, _} = application:ensure_all_started(brod),
    
    {ok, Values} = application:get_env(emqx_kafka, values),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values),
    %% PartitionStrategy= proplists:get_value(partition_strategy, Values),
    
    ClientConfig = [],
    {ok, KafkaTopic} = application:get_env(emqx_kafka, values),
    ProduceTopic = proplists:get_value(kafka_producer_topic, KafkaTopic),

    %%TODO listen message from kafka 
    %% https://github.com/emqx/emqx-delayed-publish
    %% emqx_pool:async_submit(fun emqx_broker:publish/1, [Msg])

    ok = brod:start_client(BootstrapBroker, brodClient, ClientConfig),
    ok = brod:start_producer(brodClient, ProduceTopic, _ProducerConfig = []),

    io:format("Init ekaf with ~p~n", [BootstrapBroker]).

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.authenticate', fun ?MODULE:on_client_authenticate/2),
    emqx:unhook('client.check_acl', fun ?MODULE:on_client_check_acl/5),
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
    emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
    emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqx:unhook('session.resumed', fun ?MODULE:on_session_resumed/3),
    emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/4),
    emqx:unhook('message.acked', fun ?MODULE:on_message_acked/4).


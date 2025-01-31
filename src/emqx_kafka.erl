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

-include_lib("brod/include/brod_int.hrl").

-import(string,[concat/2]).
-import(lists,[nth/2]). 

-export([load/1, unload/0]).

%% Hooks functions
-export([ %%on_client_authenticate/2
        %%, on_client_check_acl/5
        on_client_connected/4
        , on_client_disconnected/3
        % , on_client_subscribe/4
        % , on_client_unsubscribe/4
        % , on_session_created/3
        % , on_session_resumed/3
        % , on_session_terminated/3
        % , on_session_subscribed/4
        % , on_session_unsubscribed/4
        , on_message_publish/2
        % , on_message_deliver/3
        % , on_message_acked/3
        ]).

%% Called when the plugin application start
load(Env) ->
    brod_init([Env]),

    %% emqx:hook('client.authenticate', fun ?MODULE:on_client_authenticate/2, [Env]),
    %% emqx:hook('client.check_acl', fun ?MODULE:on_client_check_acl/5, [Env]),
    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    % emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
    % emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
    % emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    % emqx:hook('session.resumed', fun ?MODULE:on_session_resumed/3, [Env]),
    % emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    % emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    % emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/3, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).
    % emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [Env]),
    % emqx:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

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
    % Json = mochijson2:encode([
    %     {type, <<"connected">>},
    %     {client_id, ClientId},
    %     {client_ip, ClientIp},
    %     {cluster_node, node()},
    %     {ts, emqttd_time:now_ms()}
    % ]),
    
    % {ok, Values} = application:get_env(emqx_kafka, values),
    % ProduceTopic = proplists:get_value(kafka_producer_topic, Values),
    % {ok, CallRef} = brod:produce(brodClient, ProduceTopic, 0, <<"mykey_1">>, list_to_binary(Json)),
    % receive
    %     #brod_produce_reply{ call_ref = CallRef
    %                        , result   = brod_produce_req_acked
    %                    } ->
    %     io:format("brod_produce_reply:ok ~n"),
    %     ok
    % after 5000 ->
    %     io:format("brod_produce_reply:exit ~n"),
    %     erlang:exit(timeout)
    % end.
 
on_client_disconnected(#{clientid := ClientId}, ReasonCode, _Env) ->
    % io:format("client ~s disconnected, reason: ~w~n", [ClientId, ReasonCode]),
    % Json = mochijson2:encode([
    %     {type, <<"disconnected">>},
    %     {client_id, ClientId},
    %     {reason, ReasonCode},
    %     {cluster_node, node()},
    %     {ts, emqttd_time:now_ms()}
    % ]),
 
    % {ok, Values} = application:get_env(emqx_kafka, values),
    % ProduceTopic = proplists:get_value(kafka_producer_topic, Values),
    % {ok, CallRef} = brod:produce(brodClient, ProduceTopic, 0, <<"mykey_2">>, list_to_binary(Json)),
    % receive
    %     #brod_produce_reply{ call_ref = CallRef
    %                        , result   = brod_produce_req_acked
    %                    } ->
    %     ok
    % after 5000 ->
    %     ct:fail({?MODULE, ?LINE, timeout})
    % end,

    ok.
 
% on_client_subscribe(#{clientid := ClientId}, _Properties, RawTopicFilters, _Env) ->
%     io:format("Client(~s) will subscribe: ~p~n", [ClientId, RawTopicFilters]),
%     {ok, RawTopicFilters}.

% on_client_unsubscribe(#{clientid := ClientId}, _Properties, RawTopicFilters, _Env) ->
%     io:format("Client(~s) unsubscribe ~p~n", [ClientId, RawTopicFilters]),
%     {ok, RawTopicFilters}.

% on_session_created(#{clientid := ClientId}, SessAttrs, _Env) ->
%     io:format("Session(~s) created: ~p~n", [ClientId, SessAttrs]).

% on_session_resumed(#{clientid := ClientId}, SessAttrs, _Env) ->
%     io:format("Session(~s) resumed: ~p~n", [ClientId, SessAttrs]).

% on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
%     io:format("Session(~s) subscribe ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

% on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
%     io:format("Session(~s) unsubscribe ~s with opts: ~p~n", [ClientId, Topic, Opts]).

% on_session_terminated(#{clientid := ClientId}, ReasonCode, _Env) ->
%     io:format("Session(~s) terminated: ~p.", [ClientId, ReasonCode]).

%% transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #message{qos = Qos,
                        %% retain  = Retain,
                        from = ClientId,
                        topic   = Topic,
                        payload = Payload,
                        timestamp = Timestamp
						}, _Env) ->
    io:format("publish ~s~n", [emqx_message:format(Message)]),
    % {ok, Message}.
    %TODO emqx_json:encode
    Json = jsone:encode([
			      {topic, Topic},
			      {client_id, ClientId},
			      {payload, Payload},
                  {node, node()}
                %   {ts, Timestamp}
                 ]),
    
                 
    {ok, Values} = application:get_env(emqx_kafka, values),
    ProduceTopic = proplists:get_value(kafka_producer_topic, Values),
    
    io:format("json ~s: ~s~n", [ProduceTopic, Json]),
    % mykey_3 is partition key
    ok = brod:produce_sync(brodClient, ProduceTopic, 0, <<"mykey_3">>, Json),
    % ok = brod:produce_sync(brodClient, <<"kafka">>, 0, <<"mykey_3">>, <<"value">>),
 
    {ok, Message}.

% on_message_deliver(#{clientid := ClientId}, Message, _Env) ->
%     io:format("Deliver message to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
%     {ok, Message}.

% on_message_acked(#{clientid := ClientId}, Message, _Env) ->
%     io:format("Session(~s) acked message: ~s~n", [ClientId, emqx_message:format(Message)]),
%     {ok, Message}.

%% ===================================================================
%% brod_init https://github.com/klarna/brod
%% ===================================================================
brod_init(_Env) ->
    {ok, _} = application:ensure_all_started(brod),
    
    {ok, Values} = application:get_env(emqx_kafka, values),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values),
    %% PartitionStrategy= proplists:get_value(partition_strategy, Values),
    
    ClientConfig = [
      {query_api_versions, false}
    , {auto_start_producers, true}
    , { reconnect_cool_down_seconds, 10}],
    ok = brod:start_client(BootstrapBroker, brodClient, ClientConfig),

    %% init consumer
    brod_consumer(_Env),

    %% init the producer
    ProduceTopic = proplists:get_value(kafka_producer_topic, Values),
    io:format("topic: ~s~n", [ProduceTopic]),

    ok = brod:start_producer(brodClient, ProduceTopic, _ProducerConfig = []),
    % ok = brod:produce_sync(brodClient, ProduceTopic, 0, <<"key2">>, <<"value2">>),
    io:format("Init brod with ~p~n", BootstrapBroker).

brod_consumer(_Env) ->
    {ok, Values} = application:get_env(emqx_kafka, values),
    ConsumerTopic = proplists:get_value(kafka_consumer_topic, Values),

    SubscriberCallbackFun = fun(Partition, 
                                Msg = #kafka_message{
                                    value = Payload
                                }, 
                                ShellPid = CallbackState) -> 
        io:format("consumer: ~p~n", [Msg]),

        %%TODO listen message from kafka 
        %% https://github.com/emqx/emqx-delayed-publish
        JsonObject = emqx_json:decode(Payload),
        
        Topic = proplists:get_value(<<"topic">>, JsonObject),
        Data = proplists:get_value(<<"payload">>, JsonObject),

        %% emqx_pool:async_submit(fun emqx_broker:publish/1, [Msg])

        % emqx_pool:async_submit(fun emqx_broker:publish/1, emqx_message:make(<<"/topic">>, <<"hello">>)),
        emqx_broker:safe_publish(emqx_message:make(Topic, Data)),

        ShellPid ! Msg, {ok, ack, CallbackState} 
    end,

    brod_topic_subscriber:start_link(brodClient, ConsumerTopic, all,
                                 _ConsumerConfig=[{begin_offset, earliest}],
                                 _CommittdOffsets=[], message, SubscriberCallbackFun,
                                 _CallbackState=self()),

    io:format("topic: ~s~n", [ConsumerTopic]).


%% Called when the plugin application stop
unload() ->
    % emqx:unhook('client.authenticate', fun ?MODULE:on_client_authenticate/2),
    % emqx:unhook('client.check_acl', fun ?MODULE:on_client_check_acl/5),
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    % emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
    % emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
    % emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
    % emqx:unhook('session.resumed', fun ?MODULE:on_session_resumed/3),
    % emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    % emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    % emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/3),
    % emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/4),
    % emqx:unhook('message.acked', fun ?MODULE:on_message_acked/4).
    brod:stop_client(brodClient).
    


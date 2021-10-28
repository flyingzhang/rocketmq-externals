/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.iot.storage.rocketmq;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.List;
import java.util.Optional;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.LitePullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.iot.common.config.MqttBridgeConfig;
import org.apache.rocketmq.iot.connection.client.Client;
import org.apache.rocketmq.iot.protocol.mqtt.constant.MsgPropertyKeyConstant;
import org.apache.rocketmq.iot.protocol.mqtt.data.Subscription;
import org.apache.rocketmq.iot.storage.subscription.SubscriptionStore;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerPullTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerPullTask.class);

    private MqttBridgeConfig bridgeConfig;
    private MessageQueue messageQueue;
    private long offset;
    private boolean isRunning;
    private DefaultMQPullConsumer pullConsumer;
    private SubscriptionStore subscriptionStore;

    public ConsumerPullTask(MqttBridgeConfig bridgeConfig, MessageQueue messageQueue,
                            TopicOffset topicOffset, SubscriptionStore subscriptionStore) {
        this.bridgeConfig = bridgeConfig;
        this.messageQueue = messageQueue;
        this.offset = topicOffset.getMaxOffset();
        this.subscriptionStore = subscriptionStore;
    }

    @Override
    public void run() {
        this.isRunning = true;
        logger.info("{} task is running.", messageQueue.toString());
        try {
            startPullConsumer();
            while (isRunning) {
                pullMessages();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startPullConsumer() throws MQClientException {
        if (bridgeConfig.isRmqCredentialValid()) {
            SessionCredentials sessionCredentials = new SessionCredentials(bridgeConfig.getRmqAccessKey(), bridgeConfig.getRmqSecretKey());
            RPCHook rpcHook = new AclClientRPCHook(sessionCredentials);
            this.pullConsumer = new DefaultMQPullConsumer(rpcHook);
        } else {
            this.pullConsumer = new DefaultMQPullConsumer();
        }
        this.pullConsumer.setConsumerGroup(bridgeConfig.getRmqConsumerGroup());
        this.pullConsumer.setMessageModel(MessageModel.CLUSTERING);
        this.pullConsumer.setNamesrvAddr(bridgeConfig.getRmqNamesrvAddr());
        this.pullConsumer.setInstanceName(this.messageQueue.toString());
        this.pullConsumer.start();
    }

    private void pullMessages() {
        try {
            PullResultExt pullResult = (PullResultExt) pullConsumer.pullBlockIfNotFound(messageQueue,
                    "*", offset, bridgeConfig.getRmqConsumerPullNums());
            switch (pullResult.getPullStatus()) {
                case FOUND:
                    offset = pullResult.getNextBeginOffset();
                    sendSubscriptionClient(pullResult.getMsgFoundList());
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            logger.error("consumer pull task messages exception, messageQueue: {}", messageQueue.toString(), e);
        }
    }

    private void sendSubscriptionClient(List<MessageExt> messageExtList) {
        for (MessageExt messageExt : messageExtList) {
            //TODO:
            // 从这里的代码可以看出，当前这个代理不适合用于生产环境下的 异构生产者发送数据。暂时先使之可以以极简方式下发。
            // 但需要注意的是：1. 不能发送太大的包;（没有拆包处理） 2. 暂时还没有Qos > 0的支持（需要 packet id. 为此需要添加对每client的packet 维护工作）
            boolean isDup = Boolean.parseBoolean(Optional.ofNullable(messageExt.getUserProperty(MsgPropertyKeyConstant.MSG_IS_DUP)).orElse("false"));
            MqttQoS qosLevel = MqttQoS.valueOf(Integer.parseInt(Optional.ofNullable(messageExt.getUserProperty(MsgPropertyKeyConstant.MSG_QOS_LEVEL)).orElse("0")));
            boolean isRetain = Boolean.parseBoolean(Optional.ofNullable(messageExt.getUserProperty(MsgPropertyKeyConstant.MSG_IS_RETAIN)).orElse("false"));
            // remainingLength
            int remainingLength = Integer.parseInt(Optional.ofNullable(messageExt.getUserProperty(MsgPropertyKeyConstant.MSG_REMAINING_LENGTH)).orElse("0"));
            int packetId = Integer.parseInt(Optional.ofNullable(messageExt.getUserProperty(MsgPropertyKeyConstant.MSG_PACKET_ID)).orElse("1"));

            String mqttTopic = Optional.ofNullable(messageExt.getUserProperty(MsgPropertyKeyConstant.MQTT_TOPIC)).orElse(messageExt.getTopic());

            byte[] body = messageExt.getBody();

            List<Subscription> subscriptionList = subscriptionStore.get(mqttTopic);
            if (subscriptionList.isEmpty()) {
                return;
            }

            for (Subscription subscription : subscriptionList) {
                ByteBuf buffer = Unpooled.buffer();
                buffer.writeBytes(body);
                MqttPublishMessage msg = new MqttPublishMessage(
                        new MqttFixedHeader(
                                MqttMessageType.PUBLISH,
                                isDup,
                                qosLevel,
                                isRetain,
                                remainingLength
                        ),
                        new MqttPublishVariableHeader(
                                mqttTopic,
                                packetId
                        ),
                        buffer
                );

                Client subscriptionClient = subscription.getClient();
                subscriptionClient.getCtx().writeAndFlush(msg);
            }
        }
    }

    public void stop() {
        this.isRunning = false;
        if (this.pullConsumer != null) {
            this.pullConsumer.shutdown();
        }
    }
}

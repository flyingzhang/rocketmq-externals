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

package org.apache.rocketmq.iot.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.iot.protocol.mqtt.constant.MqttConstant;
import org.apache.rocketmq.iot.protocol.rocketmq.RmqSubscription;

public class MqttUtil {

    @Deprecated
    public static String getRootTopic(String mqttTopic) {
        // return mqttTopic.split(MqttConstant.SUBSCRIPTION_SEPARATOR)[0];
        return getRmqSubscription(mqttTopic).getTopic();
    }

    public static String createInstanceName(String value) {
        return String.valueOf((value + System.currentTimeMillis()).hashCode());
    }

    public static RmqSubscription getRmqSubscription(String mqttTopic) {
        // TODO: optimise this part.
        // /tenant-xxx/{biz-topic}/{bizKey}
        mqttTopic =  mqttTopic.startsWith("/") ? mqttTopic.substring(1) : mqttTopic;
        String parts[] = mqttTopic.split(MqttConstant.SUBSCRIPTION_SEPARATOR, 3);
        if (parts.length >= 3 ) {
            // FIXME: 要求至少有三个部分：{tenant}/{biz-topic}/{biz-key}, biz-key， 或者sub topic可以用通配符
            String tenant = parts[0].toLowerCase().trim();
            if (!tenant.startsWith("tenant")) {
                tenant = "tenant-" + tenant;
            }
            String bizTopic = parts[1];
            String bizKey = parts[2];
            String rmqTopic = bizTopic;
            MessageSelector messageSelector = MessageSelector.byTag(tenant);
            RmqSubscription rmqSubscription = new RmqSubscription();
            rmqSubscription.setTopic(bizTopic);
            rmqSubscription.setSelector(messageSelector.getExpression());
            rmqSubscription.setSelectorType(messageSelector.getExpressionType());
            return rmqSubscription;
        } else {
            RmqSubscription rmqSubscription = new RmqSubscription();
            rmqSubscription.setTopic(parts[0]);
            return rmqSubscription;
        }
    }

    public static String constructMqttTopic(MessageExt messageExt) {
        String tenant = messageExt.getTags();
        String bizTopic = messageExt.getTopic();
        // TODO: split biz key from message.
        String bizKey = messageExt.getUserProperty("gh-biz-key");
        if (StringUtils.isNotBlank(tenant)) {
            return String.format("%s/%s/%s", tenant, bizTopic, bizKey);
        } else {
            return messageExt.getTopic();
        }
    }
}

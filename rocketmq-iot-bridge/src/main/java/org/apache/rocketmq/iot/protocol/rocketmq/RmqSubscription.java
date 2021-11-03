package org.apache.rocketmq.iot.protocol.rocketmq;


import java.util.Map;

public class RmqSubscription {
    private String topic;
    private String selector;
    private String selectorType;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public String getSelectorType() {
        return selectorType;
    }

    public void setSelectorType(String selectorType) {
        this.selectorType = selectorType;
    }
}

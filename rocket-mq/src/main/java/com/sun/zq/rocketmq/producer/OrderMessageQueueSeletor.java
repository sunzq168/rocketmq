package com.sun.zq.rocketmq.producer;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @author：sunzheng
 * @date 2020/4/11 10:27
 */
public class OrderMessageQueueSeletor implements MessageQueueSelector {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object orderKey) {
        Integer key = Integer.parseInt(orderKey.toString());
        int idMainIndex = key/100;
        int size = mqs.size();
        int index = idMainIndex%size;
        return mqs.get(index);
    }
}

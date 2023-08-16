package com.sun.zq.rocketmq.consumer;


import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author：sunzheng
 * @date 2020/4/11 09:42
 */
@Service
public class PullConsumer {
    private static final Map<MessageQueue, Long> OFFSET_TABLE = new HashMap<MessageQueue, Long>();

    public void handlerMsg(String topic) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("sz_pull");
        consumer.start();
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
        for (MessageQueue mq : mqs) {
            System.out.println("mq size:" + mqs.size());
            long offset = consumer.fetchConsumeOffset(mq, true);
            System.out.println("consumer from queue:" + mq +"%n");
            SINGLE_MQ:
            while (true) {
                PullResult pullResult = null;
                try {
                    pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n" , pullResult);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch(pullResult.getPullStatus()) {
                        case FOUND:
                        case NO_MATCHED_MSG:
                        case OFFSET_ILLEGAL:
                        default:
                            break ;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(1*1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        consumer.shutdown();
    }

    private static long getMessageQueueOffset (MessageQueue mq) {
        Long offset = OFFSET_TABLE.get(mq);
        if (offset != null) {
            return offset;
        }
        return 0;
    }

    private void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSET_TABLE.put(mq, offset);
    }

}

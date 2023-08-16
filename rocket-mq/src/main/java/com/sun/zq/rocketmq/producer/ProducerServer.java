package com.sun.zq.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
public class ProducerServer {
    @Value("${spring.rocketmq.name-server}")
    private String namesrv;

    private DefaultMQProducer producer = null;


    @PostConstruct
    public void initMQProducer() {
        producer = new DefaultMQProducer("producerGroupName");
        producer.setNamesrvAddr(namesrv);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setInstanceName("sz_producer");

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public void send(String topic, String message, String tags, String keys) {
        Message msg = new Message(topic, tags, keys, message.getBytes());
        /**
         * 目前延迟的时间不支持任意设置，仅支持预设值的时间长度 (1s/5s/1Os/30s/Im/2m/3m/4m/5m/6m/ 7m/8m/9m/1Om/20m/30m/1h/2h)。
         * 比如 setDelayTimeLevel(3)表示延迟 10s
         */
        //msg.setDelayTimeLevel(3);
        msg.setDelayTimeSec(15);
        try {
            //SendResult send = producer.send(msg);
            SendResult send = producer.send(msg, new OrderMessageQueueSeletor(), "10");
            System.out.println(send.toString());
            return ;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void shutDownProducer() {
        if (producer != null) {
            producer.shutdown();
        }
    }

}

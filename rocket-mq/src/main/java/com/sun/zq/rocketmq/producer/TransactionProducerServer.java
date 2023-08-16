package com.sun.zq.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
public class TransactionProducerServer {
    @Value("${spring.rocketmq.name-server}")
    private String namesrv;

    private TransactionMQProducer producer = null;


    @PostConstruct
    public void initMQProducer() {
        TransactionListener listener = new TransactionListenerImpl();

        producer = new TransactionMQProducer("transaction_mq_group");
        producer.setNamesrvAddr(namesrv);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setInstanceName("sz_producer");
        producer.setTransactionListener(listener);

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000), r -> {
            Thread thread = new Thread(r);
            thread.setName("client-transaction-msg-check-thread");
            return thread;
        });
        producer.setExecutorService(executorService);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public void send(String topic, String message, String tags, String keys) {
        Message msg = new Message(topic, tags, keys, message.getBytes());
        try {
            SendResult send = producer.sendMessageInTransaction(msg, null);
            System.out.println(send.toString());
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

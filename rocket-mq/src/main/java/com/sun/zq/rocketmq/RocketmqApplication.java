package com.sun.zq.rocketmq;

import ch.qos.logback.core.util.ContextUtil;
import com.sun.zq.rocketmq.consumer.PullConsumer;
import com.sun.zq.rocketmq.consumer.PushConsumer;
import com.sun.zq.rocketmq.producer.ProducerServer;
import com.sun.zq.rocketmq.producer.TransactionProducerServer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class RocketmqApplication {

    public static void main(String[] args) throws MQClientException {
        ConfigurableApplicationContext context = SpringApplication.run(RocketmqApplication.class, args);
        // test(context);
    }

    private static void test( ConfigurableApplicationContext context) throws MQClientException {
        //ProducerServer producer = context.getBean(ProducerServer.class);
        TransactionProducerServer producer = context.getBean(TransactionProducerServer.class);
        for (int i = 0;i<10;i++) {
            producer.send("sun_test", "Hello MQ" + i, "mqTag" + i, "mqKey"+i);
            System.out.println("RocketMQApplication started!," + "Hello MQ" + i);
        }

        try {
            Thread.sleep(1*1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        PushConsumer consumer = context.getBean(PushConsumer.class);
        consumer.handlerMsg("sun_test");

        /*PullConsumer consumer = context.getBean(PullConsumer.class);
        consumer.handlerMsg("sun_test");*/
    }

}

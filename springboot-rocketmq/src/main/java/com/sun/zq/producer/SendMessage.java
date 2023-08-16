package com.sun.zq.producer;


import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rocketmq")
public class SendMessage {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @PostMapping("/send")
    public void sendMsg() {
        rocketMQTemplate.convertAndSend("springboot-rocketmq", "Hello Springboot RocketMQ");
    }
}

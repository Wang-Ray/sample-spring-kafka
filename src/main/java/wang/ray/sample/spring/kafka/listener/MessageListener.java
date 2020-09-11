package wang.ray.sample.spring.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {

    private static final Logger log = LoggerFactory.getLogger(MessageListener.class);

    @KafkaListener(id = "demo", topics = "topic.quick.demo")
    public void listen(String msgData) {
        log.info("listen receive : " + msgData);
    }

    @KafkaListener(id = "step1", topics = "topic.quick.step1")
    @SendTo("topic.quick.step2")
    public String listenAndForward(String msgData) {
        log.info("listenAndForward receive : " + msgData);
        return "forward: " + msgData;
    }

    @KafkaListener(id = "step2", topics = "topic.quick.step2")
    public void listenStep2(String msgData) {
        log.info("listenStep2 receive : " + msgData);
    }
}

package wang.ray.sample.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class SampleSpringKafkaApplicationTests {

    private static final Logger log = LoggerFactory.getLogger(SampleSpringKafkaApplication.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    public void testDemo() {
        // 不关心发送成功与否
        kafkaTemplate.send("topic.quick.demo", "this is my first demo");
        //休眠5秒，为了使监听器有足够的时间监听到topic的数据
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDemo1() {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("topic.quick.demo", "this is my first demo");
        SendResult<String, String> sendResult;
        try {
            sendResult = future.get();
            log.info("send successfully: {}", sendResult);
            //休眠5秒，为了使监听器有足够的时间监听到topic的数据
            Thread.sleep(5000);
        } catch (ExecutionException | InterruptedException e) {
            // get()抛出异常则说明发送失败
            log.error("send failure", e);
        }
    }

    @Test
    public void testDemo2() {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("topic.quick.demo", "this is my first demo");
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                // 发送成功
                log.info("success: {}", result);
            }

            @Override
            public void onFailure(Throwable ex) {
                // 发送失败
                log.error("failure", ex);
            }
        });
        //休眠5秒，为了使监听器有足够的时间监听到topic的数据
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStep() throws Exception {
        kafkaTemplate.send("topic.quick.step1", "this is my first demo");
        //休眠5秒，为了使监听器有足够的时间监听到topic的数据
        Thread.sleep(5000);
    }

}

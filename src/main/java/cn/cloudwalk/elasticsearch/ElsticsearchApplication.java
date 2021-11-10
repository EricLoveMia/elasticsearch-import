package cn.cloudwalk.elasticsearch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ElsticsearchApplication {

    public static void main(String[] args) {
        SpringApplication.run(ElsticsearchApplication.class, args);
    }

}

package cn.cloudwalk.elasticsearch.runner;

import cn.cloudwalk.elasticsearch.job.WriteElasticsearchJoB;
import cn.cloudwalk.elasticsearch.service.ElasticsearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * @ClassName WriteDataRunner
 * @Description: TODO
 * @Author YCKJ2725
 * @Date 2020/4/30
 * @Version V1.0
 **/
@Component
@Order(1)
public class WriteDataRunner implements CommandLineRunner{

    /**
     * 日志
     */
    public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ElasticsearchService elasticsearchService;

    private static Set<String> plates= new HashSet<>(16);

    @Value("${elasticsearch.auto:true}")
    private String auto;

    @Value("${elasticsearch.auto.per.size:2000}")
    private Integer size;

    @Value("${elasticsearch.auto.days}")
    private String days;

    @Value("${elasticsearch.data.dayly:20000000}")
    private Long dailyData;

    static {
    }

    @Override
    public void run(String... args) throws Exception {
        if(days != null && !"".equals(days)){
            String[] split = days.split(",");
            plates.addAll(Arrays.asList(split));
        }
        LOGGER.info("dailyData is {} ,需要写入的index有 {}",dailyData,plates.toString());
        // 等待60秒 让ES完全启动
        Thread.sleep(60000);
        if ("true".equals(auto)) {
            Set<String> notEmpty = new HashSet<>();
            // 检查数量
            for (String plate : plates) {
                long count;
                try {
                    count = elasticsearchService.getCountByData(plate);
                } catch (Exception e) {
                    LOGGER.error("count-exception", e);
                    count = 0;
                }
                LOGGER.info(plate + ":" + count);
                if (count < dailyData) {
                    int num = (int) ((dailyData - count) / 3500000);
                    if(num == 0){
                        num = 1;
                    }
                    elasticsearchService.scrollMultiThread(plate, num, size);
                }
            }
        }
    }

}

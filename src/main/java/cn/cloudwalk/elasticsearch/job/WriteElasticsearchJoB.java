package cn.cloudwalk.elasticsearch.job;

import cn.cloudwalk.elasticsearch.service.ElasticsearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName WriteElasticsearchJon
 * @Description: TODO
 * @Author YCKJ2725
 * @Date 2020/4/30
 * @Version V1.0
 **/
@Component
public class WriteElasticsearchJoB {

    /**
     * 日志
     */
    public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ElasticsearchService elasticsearchService;

    public static SimpleDateFormat formatSimple = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * @MethodName: writeData
     * @Description: 每5分钟跑一次程序 将今天的最近5分钟的数据补齐
     * @Param: []
     * @Return: void
     * @Author: YCKJ2725
     * @Date: 2020/4/30 18:02
    **/
    @Scheduled(cron = "${elasticsearch.cron}")
    public void writeData(){
        long lastTime = System.currentTimeMillis();
        long beginTime = lastTime - 5 * 60 * 1000;
        LOGGER.info("WriteElasticsearchJoB begin");
        String toIndices = formatSimple.format(new Date());
        elasticsearchService.writeDataOntimeThread(toIndices,beginTime,lastTime);
    }
}

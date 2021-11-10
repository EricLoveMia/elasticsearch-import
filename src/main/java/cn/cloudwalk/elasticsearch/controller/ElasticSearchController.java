package cn.cloudwalk.elasticsearch.controller;

import cn.cloudwalk.elasticsearch.service.ElasticsearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * @ClassName ElasticSearchController
 * @Description: TODO
 * @Author YCKJ2725
 * @Date 2020/4/27
 * @Version V1.0
 **/
@RestController
@RequestMapping("/elastic")
public class ElasticSearchController {

    @Autowired
    private ElasticsearchService elasticsearchService;

    /**
     * @MethodName: createDataForDay
     * @Description: 创建新的一天的
     * @Param: [day 哪一天, beginTime 开始时间，nums 需要的数量]
     * @Return: java.lang.String
     * @Author: YCKJ2725
     * @Date: 2020/4/27 17:16
    **/
    @GetMapping("/create/daily")
    public String createDataForDay(String day,int nums){
        try {
            elasticsearchService.createDataForDay(day,nums);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "fail";
        }
    }

    /**
     * @MethodName: createDemo
     * @Description: 创造模板
     * @Param: [day, nums]
     * @Return: java.lang.String
     * @Author: YCKJ2725
     * @Date: 2020/4/27 17:34
    **/
    @GetMapping("/create/demo")
    public String createDemo(String day,int nums){


        return "success";
    }

    @GetMapping("/create/test")
    public String createTest() throws IOException {
        elasticsearchService.readProAndWriteFakeMultiThread("ai-cloud-gateway-prod-logs-2020-04-26", 365000, 5000,
                "2020-04-23",10);
        return "success";
    }

    @GetMapping("/create/test/single")
    public String createTestSingle() throws IOException {
        elasticsearchService.readProAndWriteFake("ai-cloud-gateway-prod-packet-2021-03-06", 1050000, 5000,
                "2021-03-07",0);
        return "success";
    }


    @GetMapping("/create/test/scroll")
    public String createTestSingleScroll() throws IOException {
        elasticsearchService.scrollTow("ai-cloud-gateway-prod-packet-2020-04-26","2020-04-24",20000);
        return "success";
    }


    @GetMapping("/create/scroll/multi")
    public String createTestSingleScroll(String toIndices,int kernel,int size) throws IOException {
        elasticsearchService.scrollMultiThread(toIndices,kernel,size);
        return "success";
    }

    @GetMapping("/delete")
    public String delete(String indices) throws IOException {
        elasticsearchService.delete(indices);
        return "success";
    }
}

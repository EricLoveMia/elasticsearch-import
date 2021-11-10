package cn.cloudwalk.elasticsearch;

import cn.cloudwalk.elasticsearch.service.ElasticsearchService;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.sql.SQLOutput;
import java.text.SimpleDateFormat;
import java.util.*;

@SpringBootTest
class ElsticsearchApplicationTests {

    @Autowired
    private ElasticsearchService elasticsearchService;

    @Test
    void contextLoads() {
    }

    public SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Test
    public void testDate() throws Exception {
        long l = System.currentTimeMillis();
        System.out.println(format.format(new Date(l)));


    }

    @Test
    public void testWrite() throws IOException {
        System.out.println(Long.parseLong("10000000000"));
    }

    @Test
    public void testBulkInsert() throws IOException {
        //elasticsearchService.blukInsert();
    }

    @Test
    public void readAndWrite() throws IOException {
       // curl -k -H "Content-Type:application/json"  -XPUT https://222.178.203.245:9200/ai-cloud-gateway-prod-logs-2020-04-27/_settings -d '{ "index" : { "max_result_window" : 100000}}'
       // elasticsearchService.readAndWriteTwo();
       // elasticsearchService.readAndWriteFour();
       // elasticsearchService.readProAndWriteFake("ai-cloud-gateway-prod-logs-2020-04-26",150000,5000,"2020-04-23");
       // elasticsearchService.scrollTow("ai-cloud-gateway-prod-packet-2020-04-26","2020-04-24",10000);
    }

    @Test
    public void testSearch() throws IOException {
//        SearchHit[] search = elasticsearchService.search();
//        for (SearchHit documentFields : search) {
//            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
//            Set<Map.Entry<String, Object>> entries = sourceAsMap.entrySet();
//            Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//            while (iterator.hasNext()){
//                Map.Entry<String, Object> next = iterator.next();
//                System.out.println(next.getKey() + ":" + next.getValue());
//            }
//        }
    }
}

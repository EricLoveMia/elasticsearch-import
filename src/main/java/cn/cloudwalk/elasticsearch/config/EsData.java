package cn.cloudwalk.elasticsearch.config;

import java.util.*;

/**
 * @ClassName EsData
 * @Description: TODO
 * @Author YCKJ2725
 * @Date 2020/4/28
 * @Version V1.0
 **/
public class EsData {

    private static Set<String> plates= new HashSet<>(64);

    static{
        plates.add("ai-cloud-gateway-prod-packet-2021-07-01");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-02");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-03");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-04");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-05");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-06");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-07");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-08");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-09");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-10");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-11");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-12");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-13");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-14");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-15");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-20");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-21");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-22");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-23");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-24");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-25");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-26");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-27");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-28");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-29");
        plates.add("ai-cloud-gateway-prod-packet-2021-07-30");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-01");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-02");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-03");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-04");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-05");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-06");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-07");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-08");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-09");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-10");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-11");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-12");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-13");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-14");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-15");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-16");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-17");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-18");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-19");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-20");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-21");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-22");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-23");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-24");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-25");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-26");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-27");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-28");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-29");
        plates.add("ai-cloud-gateway-prod-packet-2021-06-30");
    }

    public static List<String> getRoundIndices(int num){
        List<String> result = new ArrayList<>(num);
        // 随机返回num 个索引
        ArrayList<String> strings = new ArrayList<>(plates);
        int size = strings.size();
        Collections.shuffle(strings);
        // 如果待选数量小于num数量，就要取余数
        for (int i = 0; i < num; i++) {
            int nextInt = new Random().nextInt(size) + 1;
            result.add(strings.get(i%nextInt));
        }
        return result;
    }
}

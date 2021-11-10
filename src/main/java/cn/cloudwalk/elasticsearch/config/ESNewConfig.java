package cn.cloudwalk.elasticsearch.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName ESConfig
 * @Description: 写出的ES配置
 * @Author YCKJ2725
 * @Date 2020/4/27
 * @Version V1.0
 **/
@Configuration
public class ESNewConfig {

    @Value("${elasticsearch.new.nodes}")
    private List<String> nodes;

    @Value("${elasticsearch.new.schema}")
    private String schema;

    @Value("${elasticsearch.max-connect-total}")
    private Integer maxConnectTotal;

    @Value("${elasticsearch.max-connect-per-route}")
    private Integer maxConnectPerRoute;

    @Value("${elasticsearch.connection-request-timeout-millis}")
    private Integer connectionRequestTimeoutMillis;

    @Value("${elasticsearch.socket-timeout-millis}")
    private Integer socketTimeoutMillis;

    @Value("${elasticsearch.connect-timeout-millis}")
    private Integer connectTimeoutMillis;


    @Bean(name = "restHighLevelClientNew")
    public RestHighLevelClient getRestHighLevelClient() {

        List<HttpHost> httpHosts = new ArrayList<>();

        for (String node : nodes) {
            try {
                String[] parts = StringUtils.split(node, ":");
                Assert.notNull(parts,"Must defined");
                Assert.state(parts.length == 2, "Must be defined as 'host:port'");
                httpHosts.add(new HttpHost(parts[0], Integer.parseInt(parts[1]), schema));
            } catch (RuntimeException ex) {
                throw new IllegalStateException(
                        "Invalid ES nodes " + "property '" + node + "'", ex);
            }
        }

        return EsClientNewBuilder.build(httpHosts)
                .setConnectionRequestTimeoutMillis(connectionRequestTimeoutMillis)
                .setConnectTimeoutMillis(connectTimeoutMillis)
                .setSocketTimeoutMillis(socketTimeoutMillis)
                .setMaxConnectTotal(maxConnectTotal)
                .setMaxConnectPerRoute(maxConnectPerRoute)
                .create();


    }
}

package cn.cloudwalk.elasticsearch.config;

import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import java.util.List;

/**
 * @ClassName EsClientBuilder
 * @Description: 构造器
 * @Author YCKJ2725
 * @Date 2020/4/27
 * @Version V1.0
 **/
public class EsClientBuilder {

    private int connectTimeoutMillis = 1000;
    private int socketTimeoutMillis = 30000;
    private int connectionRequestTimeoutMillis = 500;
    private int maxConnectPerRoute = 10;
    private int maxConnectTotal = 30;

    private final List<HttpHost> httpHosts;


    private EsClientBuilder(List<HttpHost> httpHosts) {
        this.httpHosts = httpHosts;
    }


    public EsClientBuilder setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    public EsClientBuilder setSocketTimeoutMillis(int socketTimeoutMillis) {
        this.socketTimeoutMillis = socketTimeoutMillis;
        return this;
    }

    public EsClientBuilder setConnectionRequestTimeoutMillis(int connectionRequestTimeoutMillis) {
        this.connectionRequestTimeoutMillis = connectionRequestTimeoutMillis;
        return this;
    }

    public EsClientBuilder setMaxConnectPerRoute(int maxConnectPerRoute) {
        this.maxConnectPerRoute = maxConnectPerRoute;
        return this;
    }

    public EsClientBuilder setMaxConnectTotal(int maxConnectTotal) {
        this.maxConnectTotal = maxConnectTotal;
        return this;
    }


    public static EsClientBuilder build(List<HttpHost> httpHosts) {
        return new EsClientBuilder(httpHosts);
    }


    public RestHighLevelClient create() {

        HttpHost[] httpHostArr = httpHosts.toArray(new HttpHost[0]);
        RestClientBuilder builder = RestClient.builder(httpHostArr);

        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(connectTimeoutMillis);
            requestConfigBuilder.setSocketTimeout(socketTimeoutMillis);
            requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeoutMillis);
            return requestConfigBuilder;
        });
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(maxConnectTotal);
            httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
            HostnameVerifier verifier = new HostnameVerifier() {
                @Override
                public boolean verify(String hostName, SSLSession sslSession) {
                    hostName = "*.cloudwalk.cn";
                    return SSLConnectionSocketFactory.getDefaultHostnameVerifier().verify(hostName,sslSession);
                }
            };
            httpClientBuilder.setSSLHostnameVerifier(verifier);
            return httpClientBuilder;
        });


        return new RestHighLevelClient(builder);
    }


}


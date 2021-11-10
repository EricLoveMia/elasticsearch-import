package cn.cloudwalk.elasticsearch.service;

import cn.cloudwalk.elasticsearch.config.EsData;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * @ClassName ElasticsearchService
 * @Description: TODO
 * @Author YCKJ2725
 * @Date 2020/4/27
 * @Version V1.0
 **/
@Service
public class ElasticsearchService implements DisposableBean {

    @Value("${elasticsearch.batch.write.corePoolSize:15}")
    private int corePoolSize;

    private static final RequestOptions COMMON_OPTIONS;

    private RequestOptions getNewOptions() {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        //builder.addHeader("Authorization", "Bearer " + TOKEN);
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(2000 * 1024 * 1024));
        return builder.build();
    }

    /**
     * 日志
     */
    public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        //builder.addHeader("Authorization", "Bearer " + TOKEN);
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(200 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    ThreadFactory threadFactoryDay = new ThreadFactoryBuilder().setNameFormat("daily-pool-%d").build();

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("demo-pool-%d").build();

    ExecutorService executorServiceDay = new ThreadPoolExecutor(5, 5, 0L
            , TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1024), threadFactoryDay,
            new ThreadPoolExecutor.CallerRunsPolicy());

    ExecutorService executorService = new ThreadPoolExecutor(corePoolSize, corePoolSize, 0L
            , TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2048), threadFactory,
            new ThreadPoolExecutor.CallerRunsPolicy());

    ThreadFactory threadFactoryTwo = new ThreadFactoryBuilder().setNameFormat("new-pool-%d").build();
    ExecutorService executorServiceTwo = new ThreadPoolExecutor(15, 15, 0L
            , TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(40960000), threadFactoryTwo,
            new ThreadPoolExecutor.CallerRunsPolicy());


    @Resource(name = "restHighLevelClient")
    private RestHighLevelClient client;

    @Resource(name = "restHighLevelClientNew")
    private RestHighLevelClient clientNew;

    @Value("${elasticsearch.concurrency}")
    private long concurrency;

    @Value("${elasticsearch.auto.per.size:2000}")
    private Integer autoSize;

    private static List<String> pathArray = Arrays.asList("/ai-cloud-device/",
            "/ai-cloud-face",
            "/ai-cloud-cweis",
            "/cloudwalk-smart-api-server",
            "/ai-cloud-asr",
            "/actuator/health");

    public void write() throws IOException {
        IndexRequest request = new IndexRequest(
                "posts", //index name
                "doc",  // type
                "1");   // doc id
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        RequestOptions options = RequestOptions.DEFAULT;
        IndexResponse indexResponse = client.index(request, options);

        LOGGER.info(indexResponse.toString());
    }

    public void writeMap() throws IOException {
        IndexRequest request = new IndexRequest(
                "ai-cloud-gateway-prod-packet-2020-04-27", "_doc");
        request.source(createNewIndex());
        RequestOptions options = RequestOptions.DEFAULT;
        IndexResponse indexResponse = client.index(request, options);
        LOGGER.info(indexResponse.toString());
    }

    /**
     * @MethodName: blukInsert
     * @Description: 批量插入
     * @Param: []
     * @Return: void
     * @Author: YCKJ2725
     * @Date: 2020/4/27 14:08
     **/
    public void blukInsert() throws IOException {
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("posts", "doc", "2")
                .source(XContentType.JSON, "field", "foo", "user", "kimchy"));
        request.add(new IndexRequest("posts", "doc", "3")
                .source(XContentType.JSON, "field", "bar", "user", "eric"));
        request.add(new IndexRequest("posts", "doc", "4")
                .source(XContentType.JSON, "field", "baz", "user", "eric"));
        RequestOptions options = RequestOptions.DEFAULT;
        BulkResponse bulk = client.bulk(request, options);
        LOGGER.info(bulk.toString());
    }


    public void read() throws IOException {
        GetRequest getRequest = new GetRequest(
                "posts",//index name
                "doc",  //type
                "1");   //id
        RequestOptions options = RequestOptions.DEFAULT;
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, options);
        } catch (ElasticsearchException e) {
            e.printStackTrace();
            if (e.status() == RestStatus.NOT_FOUND) {

            }
            if (e.status() == RestStatus.CONFLICT) {

            }
            return;
        }

        String index = getResponse.getIndex();
        String type = getResponse.getType();
        String id = getResponse.getId();
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            LOGGER.info("index :" + index + ",type:" + type + ",id :" + id + ",version" + version + "," +
                    "sourceAsString:" + sourceAsString + ", sourceAsMap" + sourceAsMap.toString());
        } else {
            //TODO
            LOGGER.info("111");
        }
    }

    public SearchHit[] search() {
//        SearchRequest searchRequest = new SearchRequest();  //构造search request .在这里无参，查询全部索引
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//大多数查询参数要写在searchSourceBuilder里
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());//增加match_all的条件。

        // 指定posts索引

        SearchRequest searchRequest = new SearchRequest("ai-cloud-gateway-prod-packet-2020-04-27");
        // 指定doc类型
        searchRequest.types("_doc");
        // 构造一个默认配置的对象  大多数的查询控制都可以使用SearchSourceBuilder实现
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 设置查询
        // sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        // 设置从哪里开始
        sourceBuilder.from(0);
        // 每页5条
        sourceBuilder.size(1);
        // 设置超时时间
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        // 配置好searchSourceBuilder后，将它传入searchRequest里：
        searchRequest.source(sourceBuilder);

        try {
            RequestOptions options = RequestOptions.DEFAULT;
            SearchResponse searchResponse = client.search(searchRequest, options);
            SearchHit[] hits = searchResponse.getHits().getHits();
            return hits;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public SearchHit[] searchWithQuery(String indices, int from, int size) {
        // 指定posts索引
        SearchRequest searchRequest = new SearchRequest(indices);
        // 指定doc类型
        // searchRequest.types("_doc");
        // 构造一个默认配置的对象  大多数的查询控制都可以使用SearchSourceBuilder实现
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 设置从哪里开始
        sourceBuilder.from(from);
        // 每页5条
        sourceBuilder.size(size);
        // 设置超时时间
        // sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        // 配置好searchSourceBuilder后，将它传入searchRequest里：
        searchRequest.source(sourceBuilder);
        try {
            // RequestOptions options = RequestOptions.DEFAULT;
            SearchResponse searchResponse = client.search(searchRequest, COMMON_OPTIONS);
            SearchHit[] hits = searchResponse.getHits().getHits();
            return hits;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public void readAndWrite() throws IOException {
        BulkRequest request = new BulkRequest();
        SearchHit[] search = this.search();
        IndexRequest indexRequest;
        for (SearchHit documentFields : search) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            sourceAsMap.put("postDate", "2020-04-28T02:09:49.627Z");
            indexRequest = new IndexRequest("posts", "doc")
                    .source(sourceAsMap);
            request.add(indexRequest);
        }
        // TODO 复制
        RequestOptions options = RequestOptions.DEFAULT;
        BulkResponse bulk = client.bulk(request, options);
        LOGGER.info(bulk.toString());
    }

    public void readAndWriteTwo() throws IOException {
        int from = 0;
        try {
            while (true) {
                BulkRequest request = new BulkRequest();
                SearchHit[] search = this.searchWithQuery("ai-cloud-gateway-prod-packet-2020-04-27", from, 50000);
                if (search == null) {
                    break;
                }
                IndexRequest indexRequest;
                for (SearchHit documentFields : search) {
                    Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                    // 随机时间
                    sourceAsMap.put("@timestamp", randomDate("2020-04-25", "2020-04-26"));
                    indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-2020-04-25", "_doc")
                            .source(sourceAsMap);
                    request.add(indexRequest);

                }
                RequestOptions options = RequestOptions.DEFAULT;
                BulkResponse bulk = client.bulk(request, options);
                LOGGER.info(String.valueOf(bulk.status().getStatus()));
                LOGGER.info(String.valueOf(from));
                from = from + 50000;
            }
        } catch (ElasticsearchGenerationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }

    }

    public void readAndWriteThree() throws IOException {
        int from = 0;
        try {
            while (true) {
                BulkRequest request = new BulkRequest();
                SearchHit[] search = this.searchWithQuery("ai-cloud-gateway-prod-packet-2020-04-27", from, 50000);
                if (search == null) {
                    break;
                }
                IndexRequest indexRequest;
                for (SearchHit documentFields : search) {
                    Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                    // 随机时间
                    sourceAsMap.put("@timestamp", randomDate("2020-04-25", "2020-04-26"));
                    indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-2020-04-25", "_doc")
                            .source(sourceAsMap);
                    request.add(indexRequest);

                }
                RequestOptions options = RequestOptions.DEFAULT;
                BulkResponse bulk = client.bulk(request, options);
                LOGGER.info(String.valueOf(bulk.status().getStatus()));
                LOGGER.info(String.valueOf(from));
                from = from + 50000;
            }
        } catch (ElasticsearchGenerationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }

    }

    private Map<String, Object> createNewIndex() {
        Map<String, Object> result = new HashMap<>();
        result.put("method", "POST");
        result.put("ip", "172.20.21.9");
        result.put("query", "POST /ai-cloud-cweis/cweis/faceRecog/checkFaceNew");
        result.put("env", "prod");
        result.put("type", "http");
        result.put("path", "/ai-cloud-cweis/cweis/faceRecog/checkFaceNew");
        result.put("@timestamp", "2020-04-27T02:09:47.174Z");
        result.put("appname", "ai-cloud-gateway");
        result.put("port", "8761");
        result.put("@version", "1");
        result.put("responsetime", "619");
        result.put("client_ip", "172.20.26.0");
        Map<String, Object> httpMap = new HashMap<>();
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> responseMap = new HashMap<>();
        Map<String, Object> headersMap = new HashMap<>();
        result.put("http", httpMap);
        httpMap.put("request", requestMap);
        httpMap.put("response", responseMap);
        requestMap.put("headers", headersMap);
        headersMap.put("content-length", 32814);
        headersMap.put("pragma", "no-cache");
        headersMap.put("connection", "Keep-Alive");
        headersMap.put("user-agent", "Java/1.8.0_131");
        headersMap.put("content-type", "application/x-www-form-urlencoded");
        headersMap.put("x-forwarded-for", "111.59.5.34");
        headersMap.put("accept", "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2");
        headersMap.put("host", "10.14.0.1:1443");
        headersMap.put("x-real-ip", "111.59.5.34");
        headersMap.put("x-request-id", "64e30514b02c65afdf6eee8c36071c27");
        headersMap.put("x-nginx-proxy", "true");
        responseMap.put("code", 200);

        return result;
    }

    private static Date randomDate(String beginDate, String endDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse(beginDate);
            Date end = format.parse(endDate);

            if (start.getTime() >= end.getTime()) {
                return null;
            }
            long date = random(start.getTime(), end.getTime());
            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }

    public void createDataForDay(String day, int nums) {
//        int totalNum = 0;
//        // 创建index
//        String index = "ai-cloud-gateway-prod-packet-" + day;
//        // 全天
//        if(StringUtils.isEmpty(beginTime) && StringUtils.isEmpty(endTime)){
//
//        }
    }

    public void readAndWriteFour() throws IOException {
        // 读生产的
        SearchHit[] search = this.search();

        BulkRequest request = new BulkRequest();
        IndexRequest indexRequest;
        for (SearchHit documentFields : search) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            // 随机时间
            sourceAsMap.put("@timestamp", randomDate("2020-04-24", "2020-04-25"));
            sourceAsMap.put("ip", "127.0.0.7");
            indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-2020-04-24")
                    .source(sourceAsMap);
            request.add(indexRequest);
        }
        LOGGER.info(String.valueOf(request.numberOfActions()));
        // 写
//        RequestOptions options = RequestOptions.DEFAULT;
//        BulkResponse bulk = clientNew.bulk(request, options);
//        LOGGER.info(bulk.status().getStatus());
    }


    public void readProAndWriteFake(String FormIndices, int begin, int size, String toDate, int interval) throws IOException {
        int count = 0;
        SearchHit[] search;
        BulkRequest request;
        IndexRequest indexRequest;
        Map<String, Object> sourceAsMap;
        try {
            while (true) {
                try {
                    LOGGER.info("begin : " + begin + ",size " + size + ",interval " + interval);
                    // 读生产的
                    search = this.searchWithQuery(FormIndices, begin, size);
                    if (search == null) {
                        break;
                    }
                    request = new BulkRequest();
                    for (SearchHit documentFields : search) {
                        sourceAsMap = documentFields.getSourceAsMap();
                        indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-" + toDate)
                                .source(sourceAsMap);
                        request.add(indexRequest);
                    }
                    if (request.numberOfActions() == 0) {
                        break;
                    }
                    count = count + request.numberOfActions();
                    LOGGER.info(Thread.currentThread().getName() + " total:" + count);
                    // 写
                    //        RequestOptions options = RequestOptions.DEFAULT;
                    //        BulkResponse bulk = clientNew.bulk(request, options);
                    //        LOGGER.info(bulk.status().getStatus());
                    begin = begin + size + interval;
                } catch (ElasticsearchGenerationException e) {
                    e.printStackTrace();
                    break;
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.info("Error");
            e.printStackTrace();
        }
    }


    public void readProAndWriteFakeMultiThread(String FormIndices, int begin, int size, String toDate, int threadNum) {
        int interval = size * threadNum;
        for (int i = 0; i < threadNum; i++) {
            int finalI = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    final int beginFianle = begin + size * finalI;
                    try {
                        readProAndWriteFake(FormIndices, beginFianle, size, toDate, interval);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();

    }

    public void readProAndWriteFakeScroll(String FormIndices, String toDate) throws IOException {
        // 指定posts索引
        SearchRequest searchRequest = new SearchRequest(FormIndices);
        // 指定doc类型
        // searchRequest.types("_doc");
        // 构造一个默认配置的对象  大多数的查询控制都可以使用SearchSourceBuilder实现
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(5000);
        Scroll scroll = searchRequest.source(sourceBuilder).scroll();

        SearchResponse searchResponse = client.search(searchRequest, COMMON_OPTIONS);
        SearchHit[] hits = searchResponse.getHits().getHits();

    }

    /**
     * @MethodName: scrollMultiThread
     * @Description: 多线程写入一个索引库，一个线程 表示将随机一天的数据写入toIndices中，最多30个线程
     * @Param: [toIndices, kernel]
     * @Return: void
     * @Author: YCKJ2725
     * @Date: 2020/4/29 9:32
     **/
    public void scrollMultiThread(String toIndices, int kernel, int size) {
        LOGGER.info("scrollMultiThread begin {} {}", toIndices, kernel);
        List<String> roundIndiceList = EsData.getRoundIndices(kernel);
        for (int i = 0; i < kernel; i++) {
            int finalI = i;
            executorService.execute(() -> {
                try {
                    LOGGER.info(Thread.currentThread().getName() + "begin");
                    scrollTow(roundIndiceList.get(finalI), toIndices, size);
                    LOGGER.info(Thread.currentThread().getName() + "end");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        LOGGER.info("scrollMultiThread " +
                "" +
                "" +
                "" +
                " {} {}", toIndices, kernel);
        //executorServiceTwo.shutdown();
        //executorService.shutdown();
        LOGGER.info("scrollMultiThread end {} {}", toIndices, kernel);
    }

    public void scroll(String formIndices, String toIndices) throws IOException {
        LOGGER.info(formIndices + ":" + toIndices);
        // SearchHit[] search;
        BulkRequest request;
        IndexRequest indexRequest;

        int count = 0;
        // 初始化scroll
        // 设定滚动时间间隔
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(formIndices);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 设定每次返回多少条数据
        searchSourceBuilder.size(10000);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, COMMON_OPTIONS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        count = count + searchResponse.getHits().getHits().length;
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        LOGGER.info("-----首页-----" + System.currentTimeMillis());
        request = new BulkRequest();
        for (SearchHit documentFields : searchHits) {
            indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-" + toIndices)
                    .source(documentFields.getSourceAsMap());
            request.add(indexRequest);
        }
        // 写
        // RequestOptions options = RequestOptions.DEFAULT;
        BulkResponse bulk = clientNew.bulk(request, COMMON_OPTIONS);
        LOGGER.info(bulk.status().getStatus() + ":" + System.currentTimeMillis());
        //遍历搜索命中的数据，直到没有数据
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            try {
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            count = count + searchHits.length;
            if (searchHits != null && searchHits.length > 0) {
                LOGGER.info("-----下一页-----");
                request = new BulkRequest();
                for (SearchHit documentFields : searchHits) {
                    //sourceAsMap = documentFields.getSourceAsMap();
                    indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-" + toIndices)
                            .source(documentFields.getSourceAsMap());
                    request.add(indexRequest);
                }
                // 写
                // RequestOptions options = RequestOptions.DEFAULT;
                bulk = clientNew.bulk(request, COMMON_OPTIONS);
                LOGGER.info(String.valueOf(bulk.status().getStatus()));
            }
            LOGGER.info(String.valueOf(count));
        }
        //清除滚屏
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);//也可以选择setScrollIds()将多个scrollId一起使用
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = client.clearScroll(clearScrollRequest, COMMON_OPTIONS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        client.close();
        clientNew.close();
        boolean succeeded = clearScrollResponse.isSucceeded();
        LOGGER.info("succeeded:" + succeeded);
    }


    public void scrollTow(String formIndices, String toIndices, int size) throws IOException {
        long total = 0;
        int errorTimes = 0;
        LOGGER.info(formIndices + ":" + toIndices);
        // SearchHit[] search;
        BulkRequest request;
        IndexRequest indexRequest;

        long count = 0;
        // 初始化scroll
        // 设定滚动时间间隔
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(formIndices);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 设定每次返回多少条数据
        int number = size + new Random().nextInt(size / 2);
        searchSourceBuilder.size(number);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse;
        try {
            CountRequest countRequest = new CountRequest(formIndices);
            CountResponse countResponse = client.count(countRequest, getNewOptions());
            total = countResponse.getCount();
            searchResponse = client.search(searchRequest, getNewOptions());
        } catch (IOException e) {
            LOGGER.error("formIndices:" + formIndices, e);
            return;
        }
        count = count + searchResponse.getHits().getHits().length;
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        LOGGER.info("-----首页-----" + System.currentTimeMillis());
        request = new BulkRequest();
        for (SearchHit documentFields : searchHits) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            String timestamp = (String) sourceAsMap.get("@timestamp");
            // 过滤path
            String path = (String) sourceAsMap.get("path");
            if(checkDiscardPath(path)){
                continue;
            }
            if (!StringUtils.isEmpty(timestamp)) {
                try {
                    sourceAsMap.put("@timestamp", toIndices + timestamp.substring(10));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Map<String, Object> httpMap = (Map<String, Object>) sourceAsMap.get("http");
            if (httpMap != null && httpMap.size() > 0) {
                Map<String, Object> requestMap = (Map<String, Object>) httpMap.get("request");
                if (requestMap != null && requestMap.size() > 0) {
                    requestMap.remove("body");
                    requestMap.remove("params");
                    httpMap.put("request", requestMap);
                }
                Map<String, Object> responseMap = (Map<String, Object>) httpMap.get("response");
                if (responseMap != null && responseMap.size() > 0) {
                    responseMap.remove("body");
                    httpMap.put("response", responseMap);
                }
                sourceAsMap.put("http", httpMap);
            }
            indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-" + toIndices)
                    .source(sourceAsMap);
            request.add(indexRequest);
        }
        // 写
        BulkResponse bulk = clientNew.bulk(request, getNewOptions());
        LOGGER.info(bulk.status().getStatus() + ":" + System.currentTimeMillis());
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        // 遍历搜索命中的数据，直到没有数据
        while (searchHits.length > 0) {
            try {
                scrollRequest.scroll(scroll);
                try {
                    searchResponse = client.scroll(scrollRequest, getNewOptions());
                } catch (IOException e) {
                    e.printStackTrace();
                    errorTimes++;
                    continue;
                }
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
                if (searchHits.length == 0) {
                    LOGGER.info(" searchHits is null -----  end");
                    break;
                } else {
                    LOGGER.info(" searchHits is  not null ----- " + searchHits.length);
                }
                count = count + searchHits.length;
                LOGGER.info("-----下一页-----");
                request = new BulkRequest();

                LOGGER.info("-----过滤前----- {}",searchHits.length);
                SearchHit[] finalSearchHits = checkDiscardHits(searchHits);
                LOGGER.info("-----过滤后----- {}",finalSearchHits.length);
                IndexRequest[] requests = new IndexRequest[finalSearchHits.length];
                CountDownLatch countDownLatch = new CountDownLatch(finalSearchHits.length);
                try {
                    for (int i = 0; i < finalSearchHits.length; i++) {
                        int finalI = i;
                        // 清除 request  response 中的body 减少数据量
                        executorServiceTwo.execute(() -> {
                            try {
                                Map<String, Object> sourceAsMap = finalSearchHits[finalI].getSourceAsMap();
                                String timestamp = (String) sourceAsMap.get("@timestamp");
                                sourceAsMap.put("@timestamp", toIndices + timestamp.substring(10));
                                Map<String, Object> httpMap = (Map<String, Object>) sourceAsMap.get("http");
                                if (httpMap != null && httpMap.size() > 0) {
                                    Map<String, Object> requestMap = (Map<String, Object>) httpMap.get("request");
                                    if (requestMap != null && requestMap.size() > 0) {
                                        requestMap.remove("body");
                                        requestMap.remove("params");
                                        httpMap.put("request", requestMap);
                                    }
                                    Map<String, Object> responseMap = (Map<String, Object>) httpMap.get("response");
                                    if (responseMap != null && responseMap.size() > 0) {
                                        responseMap.remove("body");
                                        httpMap.put("response", responseMap);
                                    }
                                    sourceAsMap.put("http", httpMap);
                                }
                                requests[finalI] = new IndexRequest("ai-cloud-gateway-prod-packet-" + toIndices)
                                        .source(sourceAsMap);
                                countDownLatch.countDown();
                            } catch (Exception e) {
                                LOGGER.error("", e);
                            }
                        });
                    }
                } catch (Exception e) {
                    LOGGER.error("", e);
                    Map<String, Object> sourceAsMap;
                    for (SearchHit documentFields : searchHits) {
                        sourceAsMap = documentFields.getSourceAsMap();
                        // 过滤path
                        String path = (String) sourceAsMap.get("path");
                        if(checkDiscardPath(path)){
                            continue;
                        }
                        String timestamp = (String) sourceAsMap.get("@timestamp");
                        if (!StringUtils.isEmpty(timestamp)) {
                            try {
                                sourceAsMap.put("@timestamp", toIndices + timestamp.substring(10));
                            } catch (Exception ee) {
                                LOGGER.error("", ee);
                            }
                        }
                        Map<String, Object> httpMap = (Map<String, Object>) sourceAsMap.get("http");
                        if (httpMap != null && httpMap.size() > 0) {
                            Map<String, Object> requestMap = (Map<String, Object>) httpMap.get("request");
                            if (requestMap != null && requestMap.size() > 0) {
                                requestMap.remove("body");
                                requestMap.remove("params");
                                httpMap.put("request", requestMap);
                            }
                            Map<String, Object> responseMap = (Map<String, Object>) httpMap.get("response");
                            if (responseMap != null && responseMap.size() > 0) {
                                responseMap.remove("body");
                                httpMap.put("response", responseMap);
                            }
                            sourceAsMap.put("http", httpMap);
                        }
                        indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-" + toIndices)
                                .source(sourceAsMap);
                        request.add(indexRequest);
                        countDownLatch.countDown();
                    }
                }

                try {
                    countDownLatch.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOGGER.warn("InterruptedException", e);
                }
                LOGGER.info("request {}",requests.length);
                request.add(requests);
                // 写
                LOGGER.info("begin writing to " + toIndices + ",from" + formIndices);
                bulk = clientNew.bulk(request, getNewOptions());
                LOGGER.info(bulk.status().getStatus() + ":" + System.currentTimeMillis());
                // 不开启异步，不好控制
//                clientNew.bulkAsync(request, COMMON_OPTIONS, new ActionListener<BulkResponse>() {
//                    @Override
//                    public void onResponse(BulkResponse bulkItemResponses) {
//                        LOGGER.info(String.valueOf(bulkItemResponses.status().getStatus()));
//                    }
//
//                    @Override
//                    public void onFailure(Exception e) {
//                        LOGGER.error("error", e);
//                    }
//                });
                LOGGER.info("写入" + count + ":剩余" + (total - count) + ",日期:" + toIndices);
                if (count >= total - 5000) {
                    break;
                }
                Thread.sleep(100);
            } catch (Exception e) {
                LOGGER.error( formIndices + ":发生异常：" + e.getMessage());
                errorTimes++;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    // ex.printStackTrace();
                }
                if(errorTimes > 50000){
                    break;
                }
                if (count >= (total - 10000)) {
                    break;
                }
            }
        }
        //清除滚屏
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);//也可以选择setScrollIds()将多个scrollId一起使用
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = client.clearScroll(clearScrollRequest, getNewOptions());
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert clearScrollResponse != null;
        boolean succeeded = clearScrollResponse.isSucceeded();
        LOGGER.info("succeeded:" + succeeded);
    }

    private SearchHit[] checkDiscardHits(SearchHit[] finalSearchHits) {
        LOGGER.info("checkDiscardHits begin");
        List<SearchHit> hits = new ArrayList<>();
        for (SearchHit hit : finalSearchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            // 过滤path
            String path = (String) sourceAsMap.get("path");
            if(!checkDiscardPath(path)){
                hits.add(hit);
            }
        }
        LOGGER.info("checkDiscardHits end");
        return hits.toArray(new SearchHit[hits.size()]);
    }

    /**
     * @MethodName: checkDiscardPath
     * @Description: 检查是否需要 不要的抛弃
     * @Param: [path]
     * @Return: boolean
     * @Author: YCKJ2725
     * @Date: 2021/3/19 17:50
    **/
    private boolean checkDiscardPath(String path) {
        for (String pre : pathArray) {
            if(path.contains(pre)){
                return false;
            }
        }
        return true;
    }


    @Override
    public void destroy() throws Exception {
        executorService.shutdownNow();
        executorServiceTwo.shutdownNow();
    }

    public long getCountByData(String plate) throws IOException {

        CountRequest countRequest = new CountRequest("ai-cloud-gateway-prod-packet-" + plate);
        CountResponse countResponse = clientNew.count(countRequest, COMMON_OPTIONS);
        return countResponse.getCount();
    }

    public SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    /**
     * @MethodName: writeDataOntimeThread
     * @Description: 写入方法 从beginTime 到 endTime
     * @Param: [toIndices, beginTime, endTime]
     * @Return: void
     * @Author: YCKJ2725
     * @Date: 2020/6/19 9:47
    **/
    public void writeDataOntimeThread(String toIndices, long beginTime, long endTime){
        final long beginTimeFinal = beginTime;
        final long endTimeFinal = endTime;
        // 随机找到concurrency的可读取的index
        List<String> roundIndiceList = EsData.getRoundIndices((int) concurrency + new Random().nextInt(3));
        for (String indice : roundIndiceList) {
            LOGGER.info(beginTime + ": "  + endTime);
            executorServiceDay.execute(() -> writeDataOntime(indice,beginTimeFinal,endTimeFinal,toIndices));
        }
    }

    public void writeDataOntime(String indices, long beginTime, long endTime, String toIndices) {
        int errorTimes = 0;
        // int count = 0;
        // 设定滚动时间间隔
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

        String beginStamp = getStampByTimeStamp(indices, beginTime);
        String endStamp = getStampByTimeStamp(indices, endTime);
        LOGGER.info(beginStamp + ":" + endStamp + ": toIndices : " + toIndices);
        // 指定posts索引
        SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.scroll(scroll);
        // 构造一个默认配置的对象  大多数的查询控制都可以使用SearchSourceBuilder实现
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        // 通过时间范围查询
        RangeQueryBuilder timestamp = QueryBuilders.rangeQuery("@timestamp").from(beginStamp).to(endStamp);
        boolBuilder.must(timestamp);
        sourceBuilder.query(boolBuilder);
        sourceBuilder.size(autoSize);
        SearchResponse searchResponse;
        // 配置好searchSourceBuilder后，将它传入searchRequest里
        searchRequest.source(sourceBuilder);
        IndexRequest indexRequest;
        String scrollId;
        SearchHit[] searchHits;
        BulkRequest request;
        BulkResponse bulk;
        int length = 0;
        try {
            searchResponse = client.search(searchRequest, getNewOptions());
            if(searchResponse == null){
                return;
            }
            searchHits = searchResponse.getHits().getHits();
            length = searchHits.length;
            LOGGER.info("hits:" + length);
            if(length == 0){
                LOGGER.info("searchHits.length is 0");
                return;
            }
            scrollId = searchResponse.getScrollId();
            LOGGER.info("-----首页-----" + System.currentTimeMillis());
            request = new BulkRequest();
            for (SearchHit documentFields : searchHits) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                // 过滤path
                String path = (String) sourceAsMap.get("path");
                if(checkDiscardPath(path)){
                    continue;
                }
                String timestampNew = (String) sourceAsMap.get("@timestamp");
                if (!StringUtils.isEmpty(timestampNew)) {
                    try {
                        sourceAsMap.put("@timestamp", toIndices + timestampNew.substring(10));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                Map<String, Object> httpMap = (Map<String, Object>) sourceAsMap.get("http");
                if (httpMap != null && httpMap.size() > 0) {
                    Map<String, Object> requestMap = (Map<String, Object>) httpMap.get("request");
                    if (requestMap != null && requestMap.size() > 0) {
                        requestMap.remove("body");
                        requestMap.remove("params");
                        httpMap.put("request", requestMap);
                    }
                    Map<String, Object> responseMap = (Map<String, Object>) httpMap.get("response");
                    if (responseMap != null && responseMap.size() > 0) {
                        responseMap.remove("body");
                        httpMap.put("response", responseMap);
                    }
                    sourceAsMap.put("http", httpMap);
                }
                indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-" + toIndices)
                        .source(sourceAsMap);
                request.add(indexRequest);
            }
            // 写
            bulk = clientNew.bulk(request, getNewOptions());
            LOGGER.info(bulk.status().getStatus() + ":" + System.currentTimeMillis());
        } catch (Exception e) {
            LOGGER.error("发生异常" +  e.getMessage());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // ex.printStackTrace();
            }
            return;
        }

        // 遍历搜索命中的数据，直到没有数据
        while (length > 0) {
            try {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                try {
                    searchResponse = client.scroll(scrollRequest, getNewOptions());
                } catch (IOException e) {
                    e.printStackTrace();
                    errorTimes++;
                }
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
                length = searchHits.length;
                if (length == 0) {
                    LOGGER.info(" searchHits is null -----  end");
                    break;
                } else {
                    LOGGER.info(" searchHits is  not null ----- " + length);
                }
                // count = count + length;
                LOGGER.info("-----下一页-----");
                request = new BulkRequest();
                LOGGER.info("-----过滤前----- {}",searchHits.length);
                SearchHit[] finalSearchHits = checkDiscardHits(searchHits);
                LOGGER.info("-----过滤后----- {}",finalSearchHits.length);
                IndexRequest[] requests = new IndexRequest[finalSearchHits.length];
                CountDownLatch countDownLatch = new CountDownLatch(finalSearchHits.length);
                try {
                    for (int i = 0; i < finalSearchHits.length; i++) {
                        int finalI = i;
                        // 清除 request  response 中的body 减少数据量
                        executorServiceTwo.execute(() -> {
                            try {
                                Map<String, Object> sourceAsMap = finalSearchHits[finalI].getSourceAsMap();
                                String timestampStr = (String) sourceAsMap.get("@timestamp");
                                sourceAsMap.put("@timestamp", toIndices + timestampStr.substring(10));
                                Map<String, Object> httpMap = (Map<String, Object>) sourceAsMap.get("http");
                                if (httpMap != null && httpMap.size() > 0) {
                                    Map<String, Object> requestMap = (Map<String, Object>) httpMap.get("request");
                                    if (requestMap != null && requestMap.size() > 0) {
                                        requestMap.remove("body");
                                        requestMap.remove("params");
                                        httpMap.put("request", requestMap);
                                    }
                                    Map<String, Object> responseMap = (Map<String, Object>) httpMap.get("response");
                                    if (responseMap != null && responseMap.size() > 0) {
                                        responseMap.remove("body");
                                        httpMap.put("response", responseMap);
                                    }
                                    sourceAsMap.put("http", httpMap);
                                }
                                requests[finalI] = new IndexRequest("ai-cloud-gateway-prod-packet-" + toIndices)
                                        .source(sourceAsMap);
                                countDownLatch.countDown();
                            } catch (Exception e) {
                                LOGGER.error("", e);
                            }
                        });
                    }
                } catch (Exception e) {
                    LOGGER.error("", e);
                    Map<String, Object> sourceAsMap;
                    for (SearchHit documentFields : searchHits) {
                        sourceAsMap = documentFields.getSourceAsMap();
                        // 过滤path
                        String path = (String) sourceAsMap.get("path");
                        if(checkDiscardPath(path)){
                            continue;
                        }
                        String timestampStr = (String) sourceAsMap.get("@timestamp");
                        if (!StringUtils.isEmpty(timestamp)) {
                            try {
                                sourceAsMap.put("@timestamp", toIndices + timestampStr.substring(10));
                            } catch (Exception ee) {
                                LOGGER.error("", ee);
                            }
                        }
                        Map<String, Object> httpMap = (Map<String, Object>) sourceAsMap.get("http");
                        if (httpMap != null && httpMap.size() > 0) {
                            Map<String, Object> requestMap = (Map<String, Object>) httpMap.get("request");
                            if (requestMap != null && requestMap.size() > 0) {
                                requestMap.remove("body");
                                requestMap.remove("params");
                                httpMap.put("request", requestMap);
                            }
                            Map<String, Object> responseMap = (Map<String, Object>) httpMap.get("response");
                            if (responseMap != null && responseMap.size() > 0) {
                                responseMap.remove("body");
                                httpMap.put("response", responseMap);
                            }
                            sourceAsMap.put("http", httpMap);
                        }
                        indexRequest = new IndexRequest("ai-cloud-gateway-prod-packet-" + toIndices)
                                .source(sourceAsMap);
                        request.add(indexRequest);
                        countDownLatch.countDown();
                    }
                }

                try {
                    countDownLatch.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOGGER.warn("InterruptedException", e);
                }
                request.add(requests);
                // 写
                bulk = clientNew.bulk(request, getNewOptions());
                LOGGER.info(bulk.status().getStatus() + ":" + System.currentTimeMillis());
                if(length < autoSize){
                    break;
                }
            } catch (Exception e) {
                LOGGER.error("发生异常：" + e.getMessage());
                errorTimes++;
                // 出现异常就休眠1秒
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    // ex.printStackTrace();
                }
                if (errorTimes > 2000) {
                    LOGGER.error("异常发生2000次以上，跳出循环");
                    break;
                }
            }
        }

        // 清除滚屏
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        // 也可以选择setScrollIds()将多个scrollId一起使用
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = client.clearScroll(clearScrollRequest, getNewOptions());
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert clearScrollResponse != null;
        boolean succeeded = clearScrollResponse.isSucceeded();
        LOGGER.info("succeeded:" + succeeded);

    }

    private String getStampByTimeStamp(String indices, long lastTime) {
        String formatStr = this.format.format(new Date(lastTime));
        return indices.substring(29) + formatStr.substring(10);
    }

    /**
     * @MethodName: delete
     * @Description: 删除
     * @Param: [indices]
     * @Return: void
     * @Author: YCKJ2725
     * @Date: 2020/5/6 16:47
    **/
    public void delete(String indices) throws IOException {

        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("ai-cloud-gateway-prod-packet-" + indices);
        clientNew.indices().delete(deleteRequest,getNewOptions());
    }
}


package EsTest;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.itheima.pojo.Article;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;

public class demoTest {

    //设置查询条件和结果
    private void searchQuery(QueryBuilder query) throws  Exception{
        //创建getTransortClient对象
        TransportClient transportClient = getTransportClient();
        //设置查询条件
        SearchResponse searchResponse = transportClient.prepareSearch("blog").setTypes("article").setQuery(query).get();
        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("命中数:"+totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while(iterator.hasNext()){
            SearchHit hitFields = iterator.next();
            String sourceAsString = hitFields.getSourceAsString();
            System.out.println(sourceAsString);
            System.out.println(hitFields.getId());
            System.out.println(hitFields.getSourceAsMap().get("id"));
            System.out.println(hitFields.getSourceAsMap().get("title"));
            System.out.println(hitFields.getSourceAsMap().get("content"));
        }
        //关闭对象
        transportClient.close();
    }


    //创建集群
    public static TransportClient getTransportClient() throws  Exception{
        //1 : 创建一个settings对象，相当于是一个配置信息，主要配置集群的名字
        Settings settings = Settings.builder().put("cluster.name","cluster-es").build();
        //2：创建一个客户端对象
        PreBuiltTransportClient transportClient = new PreBuiltTransportClient(settings);
        //3：设置集群的信息
        transportClient.addTransportAddress(new
                InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        //transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        //transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        return transportClient;
    }


    //创建索引库
    @Test
    public void createClient() throws  Exception{
        //创建getTransortClient对象
        TransportClient transportClient = getTransportClient();
        //创建一个索引库
        CreateIndexResponse response = transportClient.admin().indices().prepareCreate("blog").get();
        System.out.println(response.isAcknowledged());
        //关闭对象
        transportClient.close();
    }

    //创建Mappring规则,
    // 注意点①：mapping规则写好后，索引库不会自动创建,需要提前创建好
    //注意点②：无法更新已经建好的索引库mapping规则
    @Test
    public void createMapping() throws Exception{
        //创建getTransortClient对象
        TransportClient transportClient = getTransportClient();
        //创建索引库
        CreateIndexResponse response = transportClient.admin().indices().prepareCreate("blog").get();
        //如果索引库创建成功，则创建mapping规则
        if (response.isAcknowledged()){
            //创建规则
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                    .startObject("content")
                    .startObject("properties")
                    .startObject("id").field("type", "long").field("store", true).endObject()
                    .startObject("title").field("type", "text").field("store", true).field("analyzer", "ik_smart").endObject()
                    .startObject("content").field("type", "text").field("store", true).field("analyzer", "ik_max_word").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            //将规则加入索引库中
            PutMappingResponse putMappingResponse = transportClient.admin().indices().preparePutMapping("blog").setType("content").setSource(contentBuilder).get();
            //打印状态
            System.out.println(putMappingResponse.isAcknowledged());
        }
        //关闭对象
        transportClient.close();
    }

    //创建索引库数据,不会覆盖原有的数据
    @Test
    public void createIndex() throws  Exception{
        TransportClient transportClient = getTransportClient();
        //2：创建document
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("id",1)
                .field("title","elasticsearch是一个基于lucene的搜索服务")
                .field("content","ElasticSearch是一个基于Lucene的搜索服务器。" +
                        "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。" +
                        "Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，" +
                        "是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，" +
                        "可靠，快速，安装使用方便。")
                .endObject();
        //3：创建index
        transportClient.prepareIndex("blog", "article", "2").setSource(contentBuilder).get();
        //4：关闭资源
        transportClient.close();
    }

    //面向对象方式创建索引库数据,不会覆盖原有的数据
    @Test
    public void addIndex() throws  Exception{
        TransportClient transportClient = getTransportClient();
        // 1：创建一个client对象
        // 2：创建一个文档对象
        Article article = new Article();
        article.setId(3L);
        article.setTitle("js");
        article.setContent("js content ");

        //将数据转成json字符串
        //import com.fasterxml.jackson.databind.ObjectMapper包;
        ObjectMapper mappers = new ObjectMapper();
        String json = mappers.writeValueAsString(article);

        // 3：把文档对象添加索引库
        IndexResponse response = transportClient.prepareIndex()
                .setIndex("blog2")
                .setType("article")
                .setId(article.getId()+"")//指定ID
                .setSource(json, XContentType.JSON).get();

        //打印创建状态
        System.out.println(response.status());
        // 4：关闭
        transportClient.close();
    }

    //修改索引库数据，有id就是修改，没有id就是添加
    @Test
    public void updataIndex() throws  Exception{
        TransportClient transportClient = getTransportClient();
        // 1：创建一个client对象
        // 2：创建一个文档对象
        Article article = new Article();
        article.setId(3L);
        article.setTitle("js-6666666");
        article.setContent("js content-666666 ");

        //将数据转成json字符串
        //import com.fasterxml.jackson.databind.ObjectMapper包;
        ObjectMapper mappers = new ObjectMapper();
        String json = mappers.writeValueAsString(article);

        // 3：把文档对象添加索引库
        IndexResponse response = transportClient.prepareIndex()
                .setIndex("blog2")
                .setType("article")
                .setId(article.getId()+"")//指定ID
                .setSource(json, XContentType.JSON).get();

        //打印创建状态
        System.out.println(response.status());
        // 4：关闭
        transportClient.close();
    }

    //删除索引库数据
    @Test
    public void deleteIndex() throws Exception{
        //创建getTransortClient对象
        TransportClient transportClient = getTransportClient();
        //指定索引库中要删除的id数据
        DeleteResponse deleteResponse = transportClient.prepareDelete("blog2","content","AWv_xU9TjUC0ii5TPDIl").get();
        //查看删除的状态
        System.out.println(deleteResponse.status());
        //关闭对象
        transportClient.close();
    }

    //QueryBuilders.matchAllQuery()条件查询索引库中的数据
    @Test
    public void searchIndex() throws  Exception{
        //创建getTransortClient对象
        TransportClient transportClient = getTransportClient();
        //设置查询条件
        SearchResponse searchResponse = transportClient.prepareSearch("blog").setTypes("article").setQuery(QueryBuilders.matchAllQuery()).get();
        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("命中数:"+totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while(iterator.hasNext()){
            SearchHit hitFields = iterator.next();
            String sourceAsString = hitFields.getSourceAsString();
            System.out.println(sourceAsString);
            System.out.println(hitFields.getId());
            System.out.println(hitFields.getSourceAsMap().get("id"));
            System.out.println(hitFields.getSourceAsMap().get("title"));
            System.out.println(hitFields.getSourceAsMap().get("content"));
        }
        //关闭对象
        transportClient.close();
    }

    //字符串查询-querystring条件查询索引库中的数据
    @Test
    public void searchQuery() throws Exception{
        searchQuery(QueryBuilders.queryStringQuery("cluster-es"));
    }

    //词条查询-termquery
    @Test
    public void searchByTerm() throws  Exception{
        searchQuery(QueryBuilders.termQuery("id", "1"));
    }

    //ID查询
    @Test
    public void queryById() throws  Exception {
        // 设置搜索条件
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1","2");
        // 搜索结果
        searchQuery(queryBuilder);
    }


    /*============================================================*/

    //准备分页排序数据
    @Test
    public void batchIndex() throws  Exception{
        //创建getTransortClient对象
        TransportClient transportClient = getTransportClient();
        ObjectMapper objectMapper=new ObjectMapper();
        //批量插入数据
        for(int i=1;i<=100;i++){
            Article article= new Article();
            article.setId(1L + i);
            article.setTitle(i+"搜索工作其实很快乐");
            article.setContent(i+"我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我"+
                    "们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开 始并扩展"+
                    "到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些"+
                    "问题和更多的问题。");
            transportClient.prepareIndex("blog","article",i+"")
                    .setSource(objectMapper.writeValueAsString(article), XContentType.JSON).
                    get();
        }
        //关闭资源
        transportClient.close();
    }

    //分页和排序
    @Test
    public void queryandsort() throws  Exception{
        //创建getTransortClient对象
        TransportClient transportClient = getTransportClient();
        //分页、排序查询
        SearchRequestBuilder searchRequestBuilder =
                transportClient.prepareSearch("blog").setTypes("article").setQuery(QueryBuilders.matchAllQuery());
        int pageNo = 2;
        int pageSize = 5;
        if(pageNo <=0){
            pageNo = 1;
        }
        searchRequestBuilder.setFrom((pageNo - 1) * pageSize); //设置查询起始位置
        searchRequestBuilder.setSize(pageSize); //设置每页显示条数
        searchRequestBuilder.addSort("id", SortOrder.ASC); //设置排序
        SearchResponse searchResponse = searchRequestBuilder.get();//默认每页显示10条

        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("共查询" + hits.getTotalHits() + "条数据");
        for (SearchHit hit : hits) {
            System.out.println("查询结果" + hit.getSourceAsString());
            System.out.println("ID:" + hit.getSourceAsMap().get("id"));
            System.out.println("TITLE:" + hit.getSourceAsMap().get("title"));
            System.out.println("CONTENT:" + hit.getSourceAsMap().get("content"));
            System.out.println("===============================================");
        }
        //关闭资源
        transportClient.close();
    }

    //高亮
    @Test
    public void searchByHighLigther() throws  Exception{

        //创建getTransortClient对象
        TransportClient transportClient = getTransportClient();
        //搜索数据
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch("blog").setTypes("article").setQuery(QueryBuilders.termQuery("title", "搜索"));

        //设置高亮
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.field("title");
        highlightBuilder.postTags("</font>");
        searchRequestBuilder.highlighter(highlightBuilder);

        //获取查询结果
        SearchResponse searchResponse = searchRequestBuilder.get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("共搜索到" + hits.totalHits + "条结果");
        for (SearchHit hit : hits) {
            System.out.println("Sting方式打印文档搜索结果");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮结果");
            System.out.println(hit.getHighlightFields());
            System.out.println("遍历高亮结果，打印高亮片段");
            Text[] titles = hit.getHighlightFields().get("title").getFragments();
            StringBuilder builder = new StringBuilder();
            for (Text text : titles) {
                builder.append(text.string());
            }
            System.out.println(builder.toString());
            System.out.println("==================================");
        }
        //关闭资源
        transportClient.close();
    }

}

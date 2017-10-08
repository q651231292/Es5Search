package com.es5search.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import com.es5search.dao.Dao;

public class EsUtils {

	private static volatile TransportClient client;
	
	public static final String CLUSTER_NAME = "elasticsearch";
	
	public static final String HOST_IP = "192.168.1.102";
	
	public static final int TCP_PORT = 9300;
	
	static Settings  settings = Settings.builder().put("cluster.name",CLUSTER_NAME).build();
	
	public static TransportClient getClient() {
		try {
			client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST_IP),TCP_PORT));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return client;
	}
	
	public static TransportClient getSingleClient() {
		if(client==null) {
			synchronized (TransportClient.class) {
				if(client==null) {
					try {
						client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST_IP),TCP_PORT));
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return client;
	}
	
	
	public static IndicesAdminClient getAdminClient() {
		return getSingleClient().admin().indices();
	}
	//创建索引
	public static boolean createIndex(String indexName,int shards,int replicas) {
		
		Settings settings = Settings.builder()
				.put("index.number_of_shards",shards)
				.put("index.number_of_replicas",replicas)
				.build();
		CreateIndexResponse createIndexResponse = getAdminClient()
				.prepareCreate(indexName.toLowerCase())
				.setSettings(settings)
				.execute().actionGet();
		boolean isIndexCreated = createIndexResponse.isAcknowledged();
		if(isIndexCreated) {
			System.out.println("索引"+indexName+"创建成功");
		}else {
			System.out.println("索引"+indexName+"创建失败");
		}
		return isIndexCreated;
	}
	//删除索引
	public static boolean deleteIndex(String indexName) {
		DeleteIndexResponse deleteIndexResponse = getAdminClient()
		.prepareDelete(indexName.toLowerCase())
		.execute()
		.actionGet();
		boolean isIndexDeleted = deleteIndexResponse.isAcknowledged();
		if(isIndexDeleted) {
			System.out.println("索引"+indexName+"删除成功");
		}else {
			System.out.println("索引"+indexName+"删除失败");
		}
		return isIndexDeleted;
	}
	//设置mapping
	public static boolean setMapping(String indexName,String typeName,String mapping) {
		getAdminClient().preparePutMapping(indexName)
		.setType(typeName)
		.setSource(mapping, XContentType.JSON)
		.get();
		return false;
	}
	
	//创建索引
	@Test
	public void test1() {
		createIndex("twitter",3,0);
	}
	//删除索引
	@Test
	public void test2() {
		deleteIndex("twitter");
	}
	//设置mapping
	@Test
	public void test3() {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
			.startObject("properties")
			.startObject("user")
			.field("type","text")
			.endObject()
			.startObject("postDate")
			.field("type","date")
			.endObject()
			.endObject()
			.endObject();
			
			System.out.println(builder.string());
			
			setMapping("twitter", "tweet", builder.string());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//索引文档方法1
	@Test
	public void test4() {
		String json ="{"
				+ "\"user\":\"kimchy\","
				+ "\"postDate\":\"2013-01-30\","
				+ "\"message\":\"kimchy trying out Elasticsearch\""
				+ "}";
		TransportClient tc = getSingleClient();
		IndexResponse indexResponse = tc.prepareIndex("twitter","tweet","1")
		.setSource(json,XContentType.JSON)
		.get();
		RestStatus status = indexResponse.status();
		System.out.println(status.getStatus());
	}
	//索引文档方法2
	@Test
	public void test5() {
		Map<String,Object> json =new HashMap<String,Object>();
		json.put("user", "Tom");
		json.put("postDate", "2014-05-20");
		json.put("message", "Tom trying out Elasticsearch");
		
		TransportClient tc = getSingleClient();
		IndexResponse indexResponse = tc.prepareIndex("twitter","tweet","2")
		.setSource(json,XContentType.JSON)
		.get();
		RestStatus status = indexResponse.status();
		System.out.println(status.getStatus());
	}
	//索引文档方法3
	@Test
	public void test6() {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
			.field("user","Tonny")
			.field("postDate","2017-05-20")
			.field("message","Tonny trying out Elastingcsearch")
			.endObject();
			String json = builder.string();
			TransportClient tc = getSingleClient();
			IndexResponse indexResponse = tc.prepareIndex("twitter","tweet","3")
			.setSource(json,XContentType.JSON)
			.get();
			RestStatus status = indexResponse.status();
			System.out.println(status.getStatus());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//获取文档测试
	@Test
	public void test7() {
		TransportClient tc = getSingleClient();
		GetResponse getResponse = tc.prepareGet("twitter", "tweet", "2")
		.execute().actionGet();
		System.out.println(getResponse.getSourceAsString());
		System.out.println(getResponse.getSourceAsMap());
	}
	//更新文档测试
	@Test
	public void test8() {
		Map<String,Object> json =new HashMap<String,Object>();
		json.put("user", "Tom");
		json.put("postDate", "2014-05-20");
		json.put("message", "new Message");
		
		TransportClient tc = getSingleClient();
		UpdateResponse updateResponse = tc.prepareUpdate("twitter","tweet","2")
		.setDoc(json)
		.get();
		RestStatus status = updateResponse.status();
		System.out.println(status.getStatus());
	}	
	//删除文档测试
	@Test
	public void test9() {
		TransportClient tc = getSingleClient();
		DeleteResponse deleteResponse = tc.prepareDelete("twitter","tweet","1")
				.execute()
				.actionGet();
		RestStatus status = deleteResponse.status();
		System.out.println(status.getStatus());
	}	
	//搜索文档测试
	@Test
	public void test10() {
		TransportClient tc = getSingleClient();
		TermQueryBuilder termQuery = QueryBuilders.termQuery("user", "tonny");
		SearchResponse searchResponse = tc.prepareSearch("twitter")
		.setQuery(termQuery)
		.setTypes("tweet")
		.execute()
		.actionGet();
		SearchHits hits = searchResponse.getHits();
		
		System.out.println(hits.getTotalHits());
		
		for(SearchHit hit:hits) {
			System.out.println(hit.getSourceAsString());
			System.out.println(hit.getScore());
		}
	}	
	
	
	
	public static void main(String[] args) {
//		f1();
//		f2();
		//1 创建索引
		//EsUtils.createIndex("spnews", 3, 0);
		//2 创建mapping
//		try {
//			XContentBuilder builder = XContentFactory.jsonBuilder()
//					.startObject()
//					.startObject("properties")
//					.startObject("id")
//					.field("type","long")
//					.endObject()
//					.startObject("title")
//					.field("type", "text")
//					.field("analyzer","ik_max_word")
//					.field("search_analyzer","ik_max_word")
//					.field("boost",2)
//					.endObject()
//					.startObject("key_word")
//					.field("type", "text")
//					.field("analyzer", "ik_max_word")
//					.field("search_analyzer", "ik_max_word")
//					.endObject()
//					.startObject("content")
//					.field("type", "text")
//					.field("analyzer", "ik_max_word")
//					.field("search_analyzer", "ik_max_word")
//					.endObject()
//					.startObject("url")
//					.field("type", "keyword")
//					.endObject()
//					.startObject("reply")
//					.field("type", "long")
//					.endObject()
//					.startObject("source")
//					.field("type", "keyword")
//					.endObject()
//					.startObject("postdate")
//					.field("type", "date")
//					.field("format", "yyyy-MM-dd HH:mm:ss")
//					.endObject()
//					.endObject()
//					.endObject();
//					
//			System.out.println(builder.string());
//			
//			EsUtils.setMapping("spnews", "news", builder.string());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		//3 查询mysql
		Dao dao = new Dao();
		dao.getConnection();
		//4 写入es
		dao.mysqlToEs();
		
	}

	private static void f2() {
		TransportClient tc1 = getClient();
		TransportClient tc2 = getClient();
		System.out.println(tc1);
		System.out.println(tc2);
		
		System.out.println("********************");
		
		TransportClient sc1 = getSingleClient();
		TransportClient sc2 = getSingleClient();
		TransportClient sc3 = getSingleClient();
		System.out.println(sc1);
		System.out.println(sc2);
		System.out.println(sc3);
	}
	private static void f1() {
		TransportClient tc = getClient();
		GetResponse getResponse = tc.prepareGet("blog","article","1").get();
		System.out.println(getResponse.getSourceAsString());
		System.out.println(getResponse.getSourceAsMap());
		System.out.println(tc);
	}
}

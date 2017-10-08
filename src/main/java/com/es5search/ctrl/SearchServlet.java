package com.es5search.ctrl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import com.es5search.util.EsUtils;
@WebServlet(name="/SearchNews",urlPatterns="/SearchNews")
public class SearchServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
       
    public SearchServlet() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		request.setCharacterEncoding("UTF-8");
		String query = request.getParameter("query");
		//System.out.println(query);
		
		String pageNumStr = request.getParameter("pageNum");
		int pageNum = 1;
		
		if(pageNumStr!=null&&Integer.parseInt(pageNumStr)>1) {
			pageNum=Integer.parseInt(pageNumStr);
		}
		searchSpnews(query,request, pageNum);
		
		request.setAttribute("queryBack", query);
		
		request.getRequestDispatcher("result.jsp").forward(request, response);
	}

	private void searchSpnews(String query,HttpServletRequest request, int pageNum) {
		long start = System.currentTimeMillis();
		
		TransportClient tc = EsUtils.getSingleClient();
		
		MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(query, "title","content");
		
		HighlightBuilder highlightBuilder = new HighlightBuilder()
		.preTags("<span style=\"color:red\">")
		.postTags("</span>")
		.field("title")
		.field("content");
		
		SearchResponse searchResponse = tc.prepareSearch("spnews")
		.setTypes("news")
		.setQuery(multiMatchQuery)
		.highlighter(highlightBuilder)
		.setFrom((pageNum-1)*10)
		.setSize(10)
		.execute()
		.actionGet();
		
		SearchHits hits = searchResponse.getHits();
		
		ArrayList<Map<String,Object>> newsList = new ArrayList<Map<String,Object>>();
		
		for(SearchHit hit:hits) {
			Map<String, Object> news = hit.getSourceAsMap();
			HighlightField hTitle = hit.getHighlightFields().get("title");
			if(hTitle!=null) {
				Text[] fragments = hTitle.fragments();
				String hTitlesStr = "";
				for(Text fragment:fragments) {
					hTitlesStr+=fragment;
				}
				news.put("title", hTitlesStr);
			}
			HighlightField hContent = hit.getHighlightFields().get("content");
			if(hContent!=null) {
				Text[] fragments = hContent.fragments();
				String hContentsStr = "";
				for(Text fragment:fragments) {
					hContentsStr+=fragment;
				}
				news.put("content", hContentsStr);
			}
			//System.out.println(news);
			newsList.add(news);
		}
		long end = System.currentTimeMillis();
		request.setAttribute("newsList",newsList);
		request.setAttribute("totalHits",hits.getTotalHits()+"");
		request.setAttribute("totalTime",(end-start)+"");
		
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}

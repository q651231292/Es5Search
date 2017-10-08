package com.es5search.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;

import com.es5search.util.EsUtils;

public class Dao {
	
	private Connection conn;
	
	public void getConnection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String user = "root";
			String passwd = "root";
			String url = "jdbc:mysql://localhost:3306/News?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

			conn = DriverManager.getConnection(url,user,passwd);
			if(conn!=null) {
				System.out.println("mysql连接成功!");
			}else {
				System.out.println("mysql连接失败!");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void mysqlToEs() {
		String sql = "SELECT * FROM news";
		TransportClient tc = EsUtils.getSingleClient();
		Map<String,Object> map = new HashMap<String,Object>();
		try {
			PreparedStatement pstm = conn.prepareStatement(sql);
			ResultSet resultSet = pstm.executeQuery();
			while(resultSet.next()) {
				int nid = resultSet.getInt(1);
				
				map.put("id", nid);
				map.put("title", resultSet.getString(2));
				map.put("key_word", resultSet.getString(3));
				map.put("content", resultSet.getString(4));
				map.put("url", resultSet.getString(5));
				map.put("reply", resultSet.getInt(6));
				map.put("source", resultSet.getString(7));
				String postdatetime = resultSet.getTimestamp(8).toString();
				map.put("postdate", postdatetime.substring(0,postdatetime.length()-2));
				
				System.out.println(map);
				
				tc.prepareIndex("spnews", "news",String.valueOf(nid))
				.setSource(map)
				.execute()
				.actionGet();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

}

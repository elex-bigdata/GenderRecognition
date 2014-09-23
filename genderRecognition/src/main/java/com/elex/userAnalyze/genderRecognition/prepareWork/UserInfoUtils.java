package com.elex.userAnalyze.genderRecognition.prepareWork;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;

import com.elex.userAnalyze.genderRecognition.common.HbaseBasis;
import com.elex.userAnalyze.genderRecognition.common.HiveOperator;


public class UserInfoUtils {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws JSONException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws IOException, JSONException, SQLException {
		loadGenderInfo(args[0]);
	}
	
	public static void loadUserInfoTable(String userInfoFile) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(userInfoFile));
		String line = in.readLine().trim();
		HTableInterface ud = HbaseBasis.getConn().getTable(Bytes.toBytes("337user_info"));
		int i = 0;
		int j = 0;
		List<Put> list = new ArrayList<Put>();
		while (line != null){
						
			String[] values = line.split(",");
			
			if(values.length >= 13){
				i++;
				j++;
				
				Put put = new Put(Bytes.toBytes(values[2]));
				if(!values[1].equals("")){
					put.add(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes(values[1]));
				}
				
				if(!values[4].equals("")){
					put.add(Bytes.toBytes("user"), Bytes.toBytes("mail"), Bytes.toBytes(values[4]));
				}
				
				if(!values[5].equals("")){
					if(values[5].equals("1")){
						put.add(Bytes.toBytes("user"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
					}else if(values[5].equals("2")){
						put.add(Bytes.toBytes("user"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
					}
					
				}
				
				if(!values[7].equals("")){
					put.add(Bytes.toBytes("user"), Bytes.toBytes("country"), Bytes.toBytes(values[7]));
				}
				
				if(!values[8].equals("")){
					put.add(Bytes.toBytes("user"), Bytes.toBytes("lang"), Bytes.toBytes(values[8]));
				}
				
				if(!values[9].equals("")){
					put.add(Bytes.toBytes("user"), Bytes.toBytes("reg"), Bytes.toBytes(values[9]));
				}
				
				if(!values[13].equals("")){
					put.add(Bytes.toBytes("user"), Bytes.toBytes("birth"), Bytes.toBytes(values[13]));
				}
				
				if(values.length == 20){
					if(!values[19].equals("")){
						put.add(Bytes.toBytes("user"), Bytes.toBytes("sns"), Bytes.toBytes(values[19]));
					}
					
				}								
				list.add(put);
				
				if(i == 1000){
					i = 0;
					ud.put(list);
					list.clear();					
				}
								
			}
			
			line = in.readLine();
			
		}
		ud.put(list);
		ud.close();
		in.close();
		System.out.println("["+userInfoFile+"] 导入完成，共导入用户:"+j);
	}
	
	public static void loadFacebookUser(String facebookUserFile) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(facebookUserFile));
		String line = in.readLine().trim();
		HTableInterface ud = HbaseBasis.getConn().getTable(Bytes.toBytes("337user_info"));
		int i = 0;
		int j = 0;
		List<Put> list = new ArrayList<Put>();
		while (line != null){
						
			i++;
			j++;
			String id = line.substring(0, line.indexOf(" "));
			String json = line.substring(line.indexOf("{", 1), line.length());
			Put put = new Put(Bytes.toBytes(id));				
			put.add(Bytes.toBytes("user"), Bytes.toBytes("source"), Bytes.toBytes("facebook"));
			put.add(Bytes.toBytes("user"), Bytes.toBytes("s_detail"), Bytes.toBytes(json));
			
			JSONObject jsonObj;
			try {
				jsonObj = new JSONObject(json);
				if(jsonObj.has("gender")){
					Object gender = jsonObj.get("gender");
					if(gender != null){
						put.add(Bytes.toBytes("user"), Bytes.toBytes("gender"), Bytes.toBytes(gender.toString()));	
					}
				}
									
			} catch (JSONException e) {
				System.out.println("get gender from json string error!!!");
			}
						
				
			list.add(put);
			
			if(i == 1000){
				i = 0;
				ud.put(list);
				list.clear();
			}
			
			line = in.readLine();
			
		}
		ud.put(list);
		ud.close();
		in.close();
		System.out.println("["+facebookUserFile+"] 导入完成，共导入:"+j);
	}
	
	public static void loadGenderInfo(String genderInfoFile) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(genderInfoFile));
		String line = in.readLine().trim();
		HTableInterface ud = HbaseBasis.getConn().getTable(Bytes.toBytes("337user_info"));
		int i = 0;
		int j = 0;
		List<Put> list = new ArrayList<Put>();
		while (line != null){
						
			String[] values = line.split(",");
			
			if(values.length == 3){
				i++;
				j++;
				Put put = new Put(Bytes.toBytes(values[0]));				
				put.add(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes(values[1]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("gender"), Bytes.toBytes(values[2]));

				list.add(put);
				
				if(i == 1000){
					i = 0;
					ud.put(list);
					list.clear();
				}
								
			}
			
			line = in.readLine();
			
		}
		ud.put(list);
		ud.close();
		in.close();
		System.out.println("["+genderInfoFile+"] 导入完成，共导入:"+j);
	}
	
	public static int createGenderInfo() throws SQLException{
		String confSql = " set  hbase.zookeeper.quorum=dmnode3,dmnode4,dmnode5";		
		String genderSql = "INSERT OVERWRITE table 337_user_gender select uid,gender,source from 337_user_info where gender is not null";
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute(confSql);
		stmt.execute(genderSql);
		stmt.close();
		return 0;		
	}
	
	public static void loadResult(String resultFile) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(resultFile));
		String line = in.readLine().trim();
		HTableInterface ud = HbaseBasis.getConn().getTable(Bytes.toBytes("337user_info"));
		int i = 0;
		int j = 0;
		List<Put> list = new ArrayList<Put>();
		while (line != null){
						
			String[] values = line.split(",");
			
			if(values.length == 2){
				i++;
				j++;
				Put put = new Put(Bytes.toBytes(values[0]));				
				put.add(Bytes.toBytes("user"), Bytes.toBytes("gender"), Bytes.toBytes(values[1]));

				list.add(put);
				
				if(i == 1000){
					i = 0;
					ud.put(list);
					list.clear();
				}
								
			}
			
			line = in.readLine();
			
		}
		ud.put(list);
		ud.close();
		in.close();
		System.out.println("["+resultFile+"] 导入完成，共导入:"+j);
	}

}

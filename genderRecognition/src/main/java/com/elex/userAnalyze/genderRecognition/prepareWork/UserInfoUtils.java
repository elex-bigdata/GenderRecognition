package com.elex.userAnalyze.genderRecognition.prepareWork;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;

import com.elex.userAnalyze.genderRecognition.common.HbaseBasis;


public class UserInfoUtils {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws IOException, JSONException {
		loadUserInfoTable(args[0]);
		loadFacebookUser(args[1]);
		loadGenderInfo(args[3]);
	}
	
	public static void loadUserInfoTable(String userInfoFile) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(userInfoFile));
		String line = in.readLine().trim();
		HTableInterface ud = HbaseBasis.getConn().getTable(Bytes.toBytes("337user_info"));
		int i = 0;
		List<Put> list = new ArrayList<Put>();
		while (line != null){
						
			String[] values = line.split(",");
			
			if(values.length == 20){
				i++;
				
				Put put = new Put(Bytes.toBytes(values[2]));				
				put.add(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes(values[1]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("mail"), Bytes.toBytes(values[4]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("gender"), Bytes.toBytes(values[5]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("country"), Bytes.toBytes(values[7]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("lang"), Bytes.toBytes(values[8]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("reg"), Bytes.toBytes(values[9]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("birth"), Bytes.toBytes(values[13]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("sns"), Bytes.toBytes(values[19]));
				
				list.add(put);
				
				if(i == 1000){
					i = 0;
					ud.put(list);
					list.clear();
					System.out.println("Has load "+list.size() +" user");
				}
								
			}
			
			line = in.readLine();
			
		}
		ud.put(list);
		System.out.println("Has load "+list.size() +" user");
		ud.close();
		in.close();
	}
	
	public static void loadFacebookUser(String facebookUserFile) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(facebookUserFile));
		String line = in.readLine().trim();
		HTableInterface ud = HbaseBasis.getConn().getTable(Bytes.toBytes("337user_info"));
		int i = 0;
		List<Put> list = new ArrayList<Put>();
		while (line != null){
						
			i++;
			String id = line.substring(0, line.indexOf(" "));
			String json = line.substring(line.indexOf("{", 1), line.length());
			Put put = new Put(Bytes.toBytes(id));				
			put.add(Bytes.toBytes("user"), Bytes.toBytes("source"), Bytes.toBytes("facebook"));
			put.add(Bytes.toBytes("user"), Bytes.toBytes("s_detail"), Bytes.toBytes(json));
			
			JSONObject jsonObj;
			try {
				jsonObj = new JSONObject(json);
				String gender = jsonObj.get("gender").toString();
				if(gender != null){
					put.add(Bytes.toBytes("user"), Bytes.toBytes("gender"), Bytes.toBytes(gender));	
				}					
			} catch (JSONException e) {
				e.printStackTrace();
			}
						
				
			list.add(put);
			
			if(i == 1000){
				i = 0;
				ud.put(list);
				list.clear();
				System.out.println("Has load "+list.size() +" user");
			}
			
			line = in.readLine();
			
		}
		ud.put(list);
		System.out.println("Has load "+list.size() +" user");
		ud.close();
		in.close();
	}
	
	public static void loadGenderInfo(String genderInfoFile) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(genderInfoFile));
		String line = in.readLine().trim();
		HTableInterface ud = HbaseBasis.getConn().getTable(Bytes.toBytes("337user_info"));
		int i = 0;
		List<Put> list = new ArrayList<Put>();
		while (line != null){
						
			String[] values = line.split(",");
			
			if(values.length == 3){
				i++;
				
				Put put = new Put(Bytes.toBytes(values[0]));				
				put.add(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes(values[1]));
				put.add(Bytes.toBytes("user"), Bytes.toBytes("gender"), Bytes.toBytes(values[2]));

				list.add(put);
				
				if(i == 1000){
					i = 0;
					ud.put(list);
					System.out.println("Has load "+list.size() +" user");
					list.clear();
				}
								
			}
			
			line = in.readLine();
			
		}
		ud.put(list);
		System.out.println("Has load "+list.size() +" user");
		ud.close();
		in.close();
	}
	

}

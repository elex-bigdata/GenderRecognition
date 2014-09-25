package com.elex.userAnalyze.genderRecognition;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class GameBoxDataExtract {
	

	private static BufferedWriter out;
	private static int fileCount = 0;
	private static int recordCount = 0;
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		out = new BufferedWriter(new FileWriter(new File(args[0])));
		
		traverseFolder(args[1]);
		
		System.out.println("=============处理报告==============");
		System.out.println("共处理文件【"+fileCount+"】个");
		System.out.println("共产生记录【"+recordCount+"】条");
		System.out.println("=============处理报告==============");
		
		out.close();

	}
	
	public static void traverseFolder(String src) throws ZipException, IOException{
					
		File root = new File(src);
		
		if (root.exists()) {
			File[] files = root.listFiles();
			if (files.length == 0) {
				System.out.println("【"+src+"】文件夹是空的!");
				return;
			} else {
				for (File file : files) {
					if (file.isDirectory()) {
						System.out.println("开始处理文件夹:【" + file.getAbsolutePath()+"】");
						traverseFolder(file.getAbsolutePath());
					} else {
						processFile(file);
						fileCount++;
					}
				}
			}
		} else {
			System.out.println("【"+src+"】文件夹不存在!");
		} 
		
	}
	
	public static void processFile(File file) throws ZipException, IOException{
		ZipFile zf = new ZipFile(file);
		InputStream in = new BufferedInputStream(new FileInputStream(file));  
        ZipInputStream zin = new ZipInputStream(in);  
        ZipEntry ze; 
        String uid,displayname,installdate,installlocation,installsource,key,language,publisher,uninstallpath,
        uninstallstring,version,versionmajor,versionminor,windowsinstaller;
        
        while ((ze = zin.getNextEntry()) != null) {  
            if (ze.isDirectory()) {
            	
            } else {  
                System.err.println("file - " + ze.getName() + " : "  + ze.getSize() + " bytes");
                                              
                long size = ze.getSize();  
                if (size > 0) {  
                	uid = ze.getName().substring(3, ze.getName().length()-4);
                	
                    BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(ze)));  
                    String line;  
                    while ((line = br.readLine()) != null) {
                    	try {
							JSONObject json = new JSONObject(line);
							if(!json.isNull("uninstall")){
								JSONObject json2 = json.getJSONObject("uninstall");
								if(!json2.isNull("list")){
									JSONArray json3 = json2.getJSONArray("list");
									for (int i = 0; i < json3.length(); i++) {
										JSONObject json4 = json3.getJSONObject(i);
										displayname = json4.getString("displayname");
										installdate = json4.getString("installdate");
										installlocation = json4.getString("installlocation");
										installsource = json4.getString("installsource");
										key = json4.getString("key");
										language = json4.getString("language");
										publisher = json4.getString("publisher");
										uninstallpath = json4.getString("uninstallpath");
										uninstallstring = json4.getString("uninstallstring");
										version = json4.getString("version");
										versionmajor = json4.getString("versionmajor");
										versionminor = json4.getString("versionminor");
										windowsinstaller = json4.getString("windowsinstaller");
										if(uid != null && key != null){
											if(!uid.equals("") && !key.equals("")){
												out.write(uid+","+displayname+","+installdate+","+installlocation+","+installsource+","+key+
														","+language+","+publisher+","+uninstallpath+","+uninstallstring+","+version+","+
														versionmajor+","+versionminor+","+windowsinstaller+"\r\n");
												recordCount++;
											}
										}
									}
								}
							}
						} catch (JSONException e) {
							e.printStackTrace();
						}
                          
                    }  
                    br.close();  
                }            
            }  
        }  
        zin.closeEntry();
        zin.close();
	}

}

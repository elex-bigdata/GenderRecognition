package com.elex.userAnalyze.genderRecognition.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class PropertiesUtils {

	private static Properties pop = new Properties();
	static{
		InputStream is = null;
		try{
			is = PropertiesUtils.class.getClassLoader().getResourceAsStream("config.properties");
			pop.load(is);
		}catch(Exception e){
			e.printStackTrace();
			
		}finally{
			try {
				if(is!=null)is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
		
	
	
	public static int getMergeDays(){
		return Integer.parseInt(pop.getProperty("mergeDays"));
	}
	
	public static int getTopN(){
		return Integer.parseInt(pop.getProperty("topN"));
	}
	
	public static String getRootDir(){
		return pop.getProperty("rootdir");
	}

	public static String getHiveurl() {
		return pop.getProperty("hive.url");
	}
	
	public static String getHiveUser() {
		return pop.getProperty("hive.user");
	}

	public static String getHiveWareHouse() {
		return pop.getProperty("hive.warehouse");
	}
	
	public static String getCfNumOfRec(){
		return pop.getProperty("cf.numOfRec");
	}
	
	public static String getCfSimilarityClassname(){
		return pop.getProperty("cf.SimilarityClassname");
	}

	public static String getDataSetFile() {
		
		return pop.getProperty("dataset");
	}

	public static String getGidFile() {

		return pop.getProperty("gidfile");
	}

	public static String getUidFile() {

		return pop.getProperty("uidfile");
	}

	public static String getTrainMatrixFile() {
		return pop.getProperty("train.matrix.file");
	}

	public static String getTrainFile() {
		return pop.getProperty("train.file");
	}

	public static String getTestFile() {
		return pop.getProperty("test.file");
	}

	public static Double getTrainPercent() {
		return Double.parseDouble(pop.getProperty("train.percent"));
	}

	public static String getPredictMatrixFile() {
		return pop.getProperty("predict.matrix.file");
	}

	public static String getPredictResultFile() {
		return pop.getProperty("predict.result.file");
	}

	public static String getModelFile() {
		return pop.getProperty("model");
	}
}

package com.elex.userAnalyze.genderRecognition;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

import com.elex.userAnalyze.genderRecognition.common.PropertiesUtils;

public class AttributeExtraction {
	
	private static Map<String,Double> attributeMap;
	private static Set<String> gidSet;
	private static Set<String> uidSet;
	private static Map<String,Set<String>> idSets;
	private static Map<String,Integer> uidStrIntMap;
	private static Map<Integer,String> uidIntStrMap;
	private static Map<String,Integer> gidStrIntMap;
	private static Map<String,String> uidGenderMap;
	private static final String uidMappingFile = PropertiesUtils.getUidFile();
	private static final String gidMappingFile = PropertiesUtils.getGidFile();
	private static DecimalFormat df = new DecimalFormat("#.###");
	
	static{
		try {
			init();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		prepareDataForWeka();
	}
	
	public static Set<String> getGidSet() throws IOException {
		if(idSets != null){
			return idSets.get("gids");
		}else{
			return init().get("gids");
		}				
	}
		

	public static Set<String> getUidSet() throws IOException {
		if(idSets != null){
			return idSets.get("uids");
		}else{
			return init().get("uids");
		}				
	}
	
	public static Map<String,String> getUidGenderMap() throws IOException {
		
		return uidGenderMap;			
	}
	
	public static Map<String,Set<String>> init() throws IOException{
		
		if(idSets == null){
			uidGenderMap = new HashMap<String,String>();
			idSets = new HashMap<String,Set<String>>();
			uidSet = new HashSet<String>();
			gidSet = new HashSet<String>();
			BufferedReader in = new BufferedReader(new FileReader(new File(PropertiesUtils.getDataSetFile())));
			String line = in.readLine();
			String[] values;
			while(line != null){
				values = line.split(",");
				if(values.length == 5){
					uidSet.add(values[1]);
					gidSet.add(values[2]);
					uidGenderMap.put(values[1], values[0]);
				}
				line = in.readLine();
			}
			
			in.close();
			
			idSets.put("gids", gidSet);
			idSets.put("uids", uidSet);
			
			writeSetToFile(uidSet,uidMappingFile);
			writeSetToFile(gidSet,gidMappingFile);
			
		}
		
		return idSets;
				
	}
	
	public static void writeSetToFile(Set<String> set,String dist) throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(dist)));
		int i = 0;
		for(String gid:set){
			out.write(gid+","+i+"\r\n");		
			i++;
		}
		out.close();
	}
	
	public static Map<String,Double> getAttributeMap() throws IOException{
		
		if(attributeMap == null){
			attributeMap = new HashMap<String,Double>();
			for(String v : getGidSet()){
				attributeMap.put(v, 0D);
			}
			
		}else{
			clearValue(attributeMap);
			
		}
		return attributeMap;
		
	}

	private static void clearValue(Map<String, Double> map) {
		Iterator<Entry<String, Double>> ite = map.entrySet().iterator();
		while(ite.hasNext()){
			ite.next().setValue(0D);
		}
	}
	
	public static Map<String,Integer> getUidStrIntMap() throws IOException{
		if(uidStrIntMap==null){		      
			uidStrIntMap = readIdMapFile(uidMappingFile);
		}
		return uidStrIntMap;
	}
	
	public static Map<Integer,String> getUidIntStrMap() throws IOException{
		if(uidIntStrMap==null){		      
			uidIntStrMap = readIntStrIdMapFile(uidMappingFile);
		}
		return uidIntStrMap;
	}
	
	
	public static Map<String,Integer> getGidStrIntMap() throws IOException{
		if(gidStrIntMap==null){    
			gidStrIntMap = readIdMapFile(gidMappingFile);
		}
		return gidStrIntMap;
	}
	
	public static Map<String,Integer> readIdMapFile(String src) throws IOException{
		Map<String,Integer> idMap = new HashMap<String,Integer>();
		BufferedReader reader = new BufferedReader(new FileReader(new File(src))); 
		String line =reader.readLine();
        while(line != null){
        	String[] vList = line.split(",");
        	if(vList.length==2){
        		idMap.put(vList[0],Integer.parseInt(vList[1]));
        	}
        	
        	line = reader.readLine();
        }
        reader.close();
		return idMap;
		
	}
	
	public static Map<Integer,String> readIntStrIdMapFile(String src) throws IOException{
		Map<Integer,String> idMap = new HashMap<Integer,String>();
		BufferedReader reader = new BufferedReader(new FileReader(new File(src))); 
		String line =reader.readLine();
        while(line != null){
        	String[] vList = line.split(",");
        	if(vList.length==2){
        		idMap.put(Integer.parseInt(vList[1]),vList[0]);
        	}
        	
        	line = reader.readLine();
        }
        reader.close();
		return idMap;
		
	}

	public static void prepareDataForWeka() throws IOException{
		SparseMatrix matrix = new SparseMatrix(getUidSet().size(),getGidSet().size());
		
		Map<String,Integer> uidStrIntMap = getUidStrIntMap();
		Map<String,Integer> gidStrIntMap = getGidStrIntMap();
		BufferedReader in = new BufferedReader(new FileReader(new File(PropertiesUtils.getDataSetFile())));
		String line = in.readLine();
		String[] values;
		while(line != null){
			values = line.split(",");
			if(values.length == 5){
				if(matrix.get(uidStrIntMap.get(values[1]), gidStrIntMap.get(values[2])) != 0){
					matrix.setQuick(uidStrIntMap.get(values[1]), gidStrIntMap.get(values[2]), matrix.get(uidStrIntMap.get(values[1]), gidStrIntMap.get(values[2]))+Double.parseDouble(values[4]));
				}else{
					matrix.setQuick(uidStrIntMap.get(values[1]), gidStrIntMap.get(values[2]), Double.parseDouble(values[4]));
				}
				
			}
			line = in.readLine();
		}
		
		in.close();
		
			
		writeMatrix(matrix);
		splitTrainAndTest(PropertiesUtils.getTrainPercent());
				
	}
	
	public static void writeMatrix(Matrix matrix) throws IOException{
		Map<Integer,String> uidIntStrMap = getUidIntStrMap();
		Map<String,String> uidGenderMap = getUidGenderMap();
		
		BufferedWriter train = new BufferedWriter(new FileWriter(new File(PropertiesUtils.getTrainMatrixFile())));
		BufferedWriter predict = new BufferedWriter(new FileWriter(new File(PropertiesUtils.getPredictMatrixFile())));
		IntWritable topic = new IntWritable();
						
	    VectorWritable vector = new VectorWritable();
	    String gender;

	    boolean flag = true;
	    
	    for (MatrixSlice slice : matrix) {
	      topic.set(slice.index());
	      vector.set(slice.vector());
	      
	      if(flag){
	    	  train.write(createHeader(vector.get())+"\r\n");
	    	  predict.write(createHeader(vector.get())+"\r\n");
	    	  flag = false;
	      }
	      	      
	      
	      //ite= vector.get().normalize().iterator();//标准化后再输出
	      
	      gender = uidGenderMap.get(uidIntStrMap.get(topic.get()));
	      
	      if(gender.equals("male") || gender.equals("female")){
	    	  writeVector(topic,vector,train,gender);
	      }else{
	    	  writeVector(topic,vector,predict,"");
	      }
	      
	    }
	    
	    train.close();
	    predict.close();
	}
	
	public static void writeVector(IntWritable topic,VectorWritable vector,BufferedWriter writer,String gender) throws IOException{
		
		Element e;
		StringBuffer sb = new StringBuffer(200);
		Iterator<Element> ite= vector.get().iterator();
	    double vSum = vector.get().zSum();	    
		sb.append(topic.get()+",");
	      while(ite.hasNext()){
	    	  e = ite.next();
	    	  sb.append(df.format(e.get()/vSum)+",");
	      }
	      sb.append(gender);
	      writer.write(sb.toString()+"\r\n");
	}
	
	public static String createHeader(Vector v){
		StringBuffer sb = new StringBuffer(200);
		Iterator<Element> ite= v.iterator();
		Element e;
		sb.append("uid"+",");
		while(ite.hasNext()){
			e = ite.next();
			sb.append(e.index()+",");
		}
		return sb.append("gender").toString();
	}
	
	public static void splitTrainAndTest(Double trainPercent) throws IOException{
		BufferedWriter train = new BufferedWriter(new FileWriter(new File(PropertiesUtils.getTrainFile())));
		BufferedWriter test = new BufferedWriter(new FileWriter(new File(PropertiesUtils.getTestFile())));
		BufferedReader all = new BufferedReader(new FileReader(new File(PropertiesUtils.getTrainMatrixFile())));
		Random random = new Random();
		String header = all.readLine()+"\r\n";
		String line = all.readLine();
		train.write(header);
		test.write(header);
		while(line != null){
			if(!line.trim().equals("")){
				if(random.nextDouble()<trainPercent){
					train.write(line+"\r\n");
				}else{
					test.write(line+"\r\n");
				}				
			}
			
			line = all.readLine();
		}
		
		train.close();
		test.close();
		all.close();
		
		
	}
}

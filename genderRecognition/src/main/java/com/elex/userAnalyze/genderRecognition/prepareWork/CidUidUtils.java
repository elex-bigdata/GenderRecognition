package com.elex.userAnalyze.genderRecognition.prepareWork;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.elex.userAnalyze.genderRecognition.common.Constants;
import com.elex.userAnalyze.genderRecognition.common.HiveOperator;
import com.elex.userAnalyze.genderRecognition.common.PropertiesUtils;

public class CidUidUtils {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		loadCUID();
	}
	
	public static int loadCUID() throws Exception{
		int a = ToolRunner.run(new Configuration(), new CidUidMappingFileGenerator(), null);
		
		String hql = "load data inpath  '"+PropertiesUtils.getRootDir()+Constants.CUID+"/part*' OVERWRITE INTO TABLE 337_cid_uid";
		int b = HiveOperator.executeHQL(hql)?0:1;
		
		String genderSql = "INSERT OVERWRITE table 337_user_gender select uid,gender,source from 337_user_info where gender is not null";
		int c = HiveOperator.executeHQL(genderSql)?0:1;
		
		return Math.max(a, Math.max(b, c));
	}
	
}

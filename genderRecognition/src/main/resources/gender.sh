#!/bin/sh
hadoop jar /home/hadoop/genderRecognition-1.0.jar com.elex.userAnalyze.genderRecognition.prepareWork.CidUidMappingFileGenerator
hive -e "load data inpath  '/337user_analyze/cuid/part*' OVERWRITE INTO TABLE 337_cid_uid"

hive -e "INSERT OVERWRITE table 337_user_gender select uid,gender,source from 337_user_info where gender is not null" -hiveconf hbase.zookeeper.quorum=dmnode3,dmnode4,dmnode5



rm -rf /home/hadoop/wuzhongju/gender_recognition

hive -e "INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/gender_recognition' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select t.gender,c.* from 337_user_gender t  right outer join (select a.uid,b.tgid,b.gt,b.hb_sum from 337_cid_uid a join webgmrec_input b on a.cid=b.uid where b.gt='m')c on t.uid =c.uid"

cat /home/hadoop/wuzhongju/gender_recognition/* >> /home/hadoop/wuzhongju/gender_recognition/dataset.csv


java -classpath .:/home/hadoop/genderRecognition-1.0.jar com.elex.userAnalyze.genderRecognition.AttributeExtraction



java -classpath .:/home/hadoop/genderRecognition-1.0.jar com.elex.userAnalyze.genderRecognition.ModelUtils

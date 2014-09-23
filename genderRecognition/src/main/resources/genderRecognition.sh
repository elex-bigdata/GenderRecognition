#!/bin/sh
#第一步：生成cookieid和uid对应关系文件，并加载到对应的hive表
hadoop jar /home/hadoop/genderRecognition-1.0.jar com.elex.userAnalyze.genderRecognition.prepareWork.CidUidMappingFileGenerator
hive -e "load data inpath  '/337user_analyze/cuid/part*' OVERWRITE INTO TABLE 337_cid_uid"
#第二步：从hbase用户注册信息表（337user_info的hive映射表337_user_info）提取已知性别的用户信息到hive的337_user_gender
hive -e "INSERT OVERWRITE table 337_user_gender select uid,gender,source from 337_user_info where gender is not null" -hiveconf hbase.zookeeper.quorum=dmnode3,dmnode4,dmnode5
#第三步：获取行为记录（并联查出性别，uid），并将获取的文件更改为配置文件对应的配置项dataset的路径和名称。
hive -e "INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/gender_recognition' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select t.gender,c.* from 337_user_gender t  right outer join (select a.uid,b.tgid,b.gt,b.hb_sum from 337_cid_uid a join webgmrec_input b on a.cid=b.uid where b.gt='m')c on t.uid =c.uid"
cat /home/hadoop/wuzhongju/gender_recognition/0* >> /home/hadoop/wuzhongju/gender_recognition/dataset.csv
#第四步：特征提取，并按是否已知性别拆分出训练集（训练集再按比例拆分为训练数据和测试数据）和待分类数据
java -classpath .:/home/hadoop/genderRecognition-1.0.jar com.elex.userAnalyze.genderRecognition.AttributeExtraction
#第五步：训练模型，数据分类，结果加载
java -classpath .:/home/hadoop/genderRecognition-1.0.jar com.elex.userAnalyze.genderRecognition.ModelUtils
package com.elex.userAnalyze.genderRecognition;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Vector;

import weka.attributeSelection.CfsSubsetEval;
import weka.attributeSelection.GreedyStepwise;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.Logistic;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;

import com.elex.userAnalyze.genderRecognition.common.PropertiesUtils;
import com.elex.userAnalyze.genderRecognition.prepareWork.UserInfoUtils;

public class ModelUtils {

	 public final static String MODEL = "/home/hadoop/wuzhongju/cls.model";
	 
	/**
	 * @param args
	 * @throws Exception
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, Exception {
		
		train();
		predict();
		UserInfoUtils.loadResult(PropertiesUtils.getPredictResultFile());

	}

	public static Instances loadDataSet(String csvFile) throws Exception {
		if (csvFile == null) {
			System.err.println("\nUsage: java LoadDataFromCsvFile <file>\n");
			System.exit(1);
		}

		CSVLoader loader = new CSVLoader();
		loader.setSource(new File(csvFile));
		Instances data = loader.getDataSet();
		
		String[] options = new String[2];
		options[0] = "-R"; // "range"
		options[1] = "1"; // first attribute
		Remove remove = new Remove(); // new instance of filter
		remove.setOptions(options); // set options
		remove.setInputFormat(data); // inform filter about dataset
		Instances newData = Filter.useFilter(data, remove); // apply filter
		
		return newData;
	}

	public static Instances useFilter(Instances data) throws Exception {


		weka.filters.supervised.attribute.AttributeSelection filter = new weka.filters.supervised.attribute.AttributeSelection();
		CfsSubsetEval eval = new CfsSubsetEval();
		GreedyStepwise search = new GreedyStepwise();
		search.setSearchBackwards(true);
		filter.setEvaluator(eval);
		filter.setSearch(search);
		filter.setInputFormat(data);
		Instances newData = Filter.useFilter(data, filter);

		return newData;
	}

	public static void train() throws Exception {
		System.out.println("Training...");


		//Instances data = useFilter(loadDataSet(trainData));
		Instances train = loadDataSet(PropertiesUtils.getTrainFile());
		Instances test = loadDataSet(PropertiesUtils.getTestFile());
		
		train.setClassIndex(train.numAttributes() - 1);
		test.setClassIndex(test.numAttributes() - 1);
		
		

		// train Logistic
		Logistic cls=new Logistic();
		// further options...
		cls.buildClassifier(train);
				
		Evaluation eval = new Evaluation(train);
		eval.evaluateModel(cls, test);
		
		System.out.println(eval.toSummaryString("\nResults\n\n", false));
		System.out.println(eval.toMatrixString());
		
		int m_NumClasses = train.numClasses();
		String [] m_ClassNames = new String [m_NumClasses];
		
		for(int j =0;j<m_NumClasses;j++){
			m_ClassNames[j] = train.classAttribute().value(j);
		}
		
		// save model + header
	    Vector v = new Vector();
	    v.add(cls);
	    v.add(new Instances(train, 0));
	    v.add(m_ClassNames);
	    SerializationHelper.write(MODEL, v);
	    
		System.out.println("Training finished!");
	}
	
	
	public static void predict() throws Exception {
	    System.out.println("Predicting...");
	    
		
		// read model and header
	    Vector v = (Vector) SerializationHelper.read(MODEL);
	    Classifier cl = (Classifier) v.get(0);	    
	    Instances header = (Instances) v.get(1);
	    String [] m_ClassNames =(String[]) v.get(2);
		
		Map<Integer,String> uidIntStrMap = AttributeExtraction.getUidIntStrMap();
		BufferedWriter result = new BufferedWriter(new FileWriter(new File(PropertiesUtils.getPredictResultFile())));		
	    BufferedReader reader = new BufferedReader(new FileReader(new File(PropertiesUtils.getPredictMatrixFile()))); 
	    reader.readLine();
		String line = reader.readLine();
		String[] attributes;
		String gender;
		int size = AttributeExtraction.getGidSet().size()+1;
		while(line != null){
			attributes = line.split(",");
			Instance curr = new Instance(size);
			curr.setDataset(header);
			for(int i=1;i<size;i++){
				curr.setValue(i-1, Double.parseDouble(attributes[i]==null?"0":attributes[i]));
			}
			int pred = (int) cl.classifyInstance(curr);
			gender = m_ClassNames[pred];
			curr.setClassValue(pred);
			result.write(uidIntStrMap.get(Integer.parseInt(attributes[0]))+","+gender+"\r\n");
			line = reader.readLine();
		}
		
		reader.close();
		result.close();

	    System.out.println("Predicting finished!");
	  }

}

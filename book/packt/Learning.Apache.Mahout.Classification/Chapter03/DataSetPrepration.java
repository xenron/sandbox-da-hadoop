package com.packt.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class DataSetPrepration {
	public static void main(String args[]){
		PrepareDataSet pds = new PrepareDataSet();
		try {
			pds.convertTargetToInteger();
			pds.dataPrepration();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void dataPrepration() throws Exception {
		// Reading the dataset created by earlier method convertTargetToInteger and here we are using google guava api's.
		List<String> result = Resources.readLines(Resources.getResource("wdbc.csv"), Charsets.UTF_8);
		//This is to remove header before the randomization process. Otherwise it can appear in the middle of dataset.
		  List<String> raw = result.subList(0, 570);
		  Random random = new Random();
		  //Shuffling the dataset.  
		  Collections.shuffle(raw, random);
		  List<String> train = raw.subList(0, 470);
		  List<String> test = raw.subList(470, 569);	
		  File trainingData = new File("<yourlocation>//wdbcTrain.csv");
		  File testData = new File("<your location>//wdbcTest.csv");
		  writeCSV(train, trainingData);
		  writeCSV(test, testData);		 	  
	}
	
	public void writeCSV(List<String> list, File file) throws IOException{
		FileWriter fw = new FileWriter(file);
		fw.write("ID_Number"+","+"Diagnosis"+","+"Radius"+","+"Texture"+","+"Perimeter"+","+"Area"+","+"Smoothness"+","+"Compactness"+","+"Concavity"
				+","+"ConcavePoints"+","+"Symmetry"+","+"Fractal_Dimension"+","+"RadiusStdError"+","+"TextureStdError"+","+"PerimeterStdError"
				+","+"AreaStdError"+","+"SmoothnessStdError"+","+"CompactnessStdError"+","+"ConcavityStdError"+","+"ConcavePointStdError"+","+
				"Symmetrystderror"+","+"FractalDimensionStderror"+","+"WorstRadius"+","+"worsttexture"+","+"worstperimeter"+","+"worstarea" +","+
						"worstsmoothness"+","+"worstcompactness"+","+"worstconcavity"+","+"worstconcavepoints"+","+"worstsymmentry"+","+"worstfractaldimensions"+"\n");
		 for(int i=0;i< list.size();i++){
	    	  fw.write(list.get(i)+"\n");
	      }
		  fw.close();
		
	}
	
	
	public void convertTargetToInteger() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("wdbc.csv"));
		String line =null;
		File wdbcData = new File("<your location to file>");
		FileWriter fw = new FileWriter(wdbcData);
		fw.write("ID_Number"+","+"Diagnosis"+","+"Radius"+","+"Texture"+","+"Perimeter"+","+"Area"+","+"Smoothness"+","+"Compactness"+","+"Concavity"
				+","+"ConcavePoints"+","+"Symmetry"+","+"Fractal_Dimension"+","+"RadiusStdError"+","+"TextureStdError"+","+"PerimeterStdError"
				+","+"AreaStdError"+","+"SmoothnessStdError"+","+"CompactnessStdError"+","+"ConcavityStdError"+","+"ConcavePointStdError"+","+
				"Symmetrystderror"+","+"FractalDimensionStderror"+","+"WorstRadius"+","+"worsttexture"+","+"worstperimeter"+","+"worstarea" +","+
						"worstsmoothness"+","+"worstcompactness"+","+"worstconcavity"+","+"worstconcavepoints"+","+"worstsymmentry"+","+"worstfractaldimensions"+"\n");

		while((line=br.readLine())!=null){
			String []parts = line.split(",");
			if(parts[1].equals("M")){
				fw.write(parts[0]+","+"0"+","+parts[2]+","+parts[3]+","+parts[4]+","+parts[5]+","+parts[6]+","+parts[7]+","+parts[8]+","+parts[9]+","+parts[10]+","+parts[11]+","
						+parts[12]+","+parts[13]+","+parts[14]+","+parts[15]+","+parts[16]+","+parts[17]+","+parts[18]+","+parts[19]+","+parts[20]+","+parts[21]+ ","+
						parts[22]+","+parts[23]+","+parts[24]+","+parts[25]+","+parts[26]+","+parts[27]+","+parts[28]+","+parts[29]+","+parts[30]+","+parts[31]+"\n");
			}
			if(parts[1].equals("B")){
				fw.write(parts[0]+","+"1"+","+parts[2]+","+parts[3]+","+parts[4]+","+parts[5]+","+parts[6]+","+parts[7]+","+parts[8]+","+parts[9]+","+parts[10]+","+parts[11]+","
						+parts[12]+","+parts[13]+","+parts[14]+","+parts[15]+","+parts[16]+","+parts[17]+","+parts[18]+","+parts[19]+","+parts[20]+","+parts[21]+ ","+
						parts[22]+","+parts[23]+","+parts[24]+","+parts[25]+","+parts[26]+","+parts[27]+","+parts[28]+","+parts[29]+","+parts[30]+","+parts[31]+"\n");
			}
			
		}
		fw.close();
		br.close();
	}

}

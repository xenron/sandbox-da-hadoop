package com.packt.spamfilter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.vectorizer.TFIDF;
import org.apache.hadoop.io.*;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;


public class TestClassifier {
	 
    public static Map<String, Integer> readDictionary(Configuration conf, Path dictionaryPath) {
        Map<String, Integer> dictionary = new HashMap<String, Integer>();
        for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictionaryPath, true, conf)) {
            dictionary.put(pair.getFirst().toString(), pair.getSecond().get());
        }
        return dictionary;
    }
 
    public static Map<Integer, Long> readDocumentFrequency(Configuration conf, Path documentFrequencyPath) {
        Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
        for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(documentFrequencyPath, true, conf)) {
            documentFrequency.put(pair.getFirst().get(), pair.getSecond().get());
        }
        return documentFrequency;
    }
 
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {          
        	System.out.println("Arguments: [model] [labelindex] [dictionnary] [documentfrequency] [new file] ");
        	return;         
        }       
        String modelPath = args[0];         
        String labelIndexPath = args[1];       
        String dictionaryPath = args[2];        
        String documentFrequencyPath = args[3];         
        String newDataPath = args[4];                
        Configuration configuration = new Configuration();      
        NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), configuration); 
        StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(model); 
        // labels is a map label => classId
        Map<Integer, String> labels = BayesUtils.readLabelIndex(configuration, new Path(labelIndexPath));
        Map<String, Integer> dictionary = readDictionary(configuration, new Path(dictionaryPath));
        Map<Integer, Long> documentFrequency = readDocumentFrequency(configuration, new Path(documentFrequencyPath));
 
        // analyzer used to extract word from mail
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);        
 
        int labelCount = labels.size();
        int documentCount = documentFrequency.get(-1).intValue();
 
        System.out.println("Number of labels: " + labelCount);
        System.out.println("Number of documents in training set: " + documentCount);
        BufferedReader reader = new BufferedReader(new FileReader(newDataPath));
        while(true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
 
            ConcurrentHashMultiset<Object> words = ConcurrentHashMultiset.create(); 
            // extract words from mail
            TokenStream ts = analyzer.tokenStream("text", new StringReader(line));         
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            int wordCount = 0;
            while (ts.incrementToken()) {
                if (termAtt.length() > 0) {
                    String word = ts.getAttribute(CharTermAttribute.class).toString();
                    Integer wordId = dictionary.get(word);
                    // if the word is not in the dictionary, skip it
                    if (wordId != null) {
                        words.add(word);
                        wordCount++;
                    }
                }
            }
            ts.close();
 
            // create vector wordId - weight using tfidf
            Vector vector = new RandomAccessSparseVector(10000);
            TFIDF tfidf = new TFIDF();
            for (Multiset.Entry entry:words.entrySet()) {
                String word =  (String)entry.getElement();
                int count = entry.getCount();
                Integer wordId = dictionary.get(word);
                Long freq = documentFrequency.get(wordId);
                double tfIdfValue = tfidf.calculate(count, freq.intValue(), wordCount, documentCount);
                vector.setQuick(wordId, tfIdfValue);
            }
            // With the classifier, we get one score for each label
            // The label with the highest score is the one the mail is more likely to
            // be associated to
            Vector resultVector = classifier.classifyFull(vector);
            double bestScore = -Double.MAX_VALUE;
            int bestCategoryId = -1;
          
            for(int i=0 ;i<resultVector.size();i++) {
            	Element e1  = resultVector.getElement(i);
                int categoryId = e1.index();
                double score = e1.get();
                if (score > bestScore) {
                    bestScore = score;
                    bestCategoryId = categoryId;
                }
                System.out.print("  " + labels.get(categoryId) + ": " + score);
            }
            System.out.println(" => " + labels.get(bestCategoryId));
        }
    }
}
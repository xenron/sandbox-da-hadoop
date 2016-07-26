package microbook.recipe.graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

public class JavaWordCount {
    private static Map<String, Integer> tokenMap = new HashMap<String, Integer>();
    
    
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("/Users/srinath/playground/hadoop-microbook/data/small/amazon-meta-small.txt"));
        String line = br.readLine();
        while (line != null) {
            StringTokenizer tokenizer = new StringTokenizer(line); 
            while(tokenizer.hasMoreTokens()){
                String token = tokenizer.nextToken(); 
                if(tokenMap.containsKey(token)){
                    Integer value = (Integer)tokenMap.get(token);
                    tokenMap.put(token, value+1);
                }else{
                    tokenMap.put(token, new Integer(1)); 
                }
                
            }
            line = br.readLine();
        }
        
        Writer writer = new BufferedWriter(new FileWriter("results.txt")); 
        
        for(Entry<String, Integer> entry: tokenMap.entrySet()){
            writer.write(entry.getKey() + "= "+ entry.getValue());
        }
    }
}

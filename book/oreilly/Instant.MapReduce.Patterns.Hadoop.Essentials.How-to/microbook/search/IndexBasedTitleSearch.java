package microbook.search;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.mahout.math.Arrays;

public class IndexBasedTitleSearch {
    private static Map<String, String[]> invertedIndex = new HashMap<String, String[]>();
    /**
     * @param args
     */

    private static Pattern parsingPattern = Pattern.compile("([^\\s]+)\\s+([^\\s]+)");

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(
                "/Users/srinath/playground/hadoop-microbook/output9/part-r-00000"));
        // BufferedReader br = new BufferedReader(new
        // FileReader("/Users/srinath/playground/debs-2013/full-game"));
        String line = br.readLine();
        while (line != null) {
            Matcher matcher = parsingPattern.matcher(line);
            if (matcher.find()) {
                String key = matcher.group(1);
                String value = matcher.group(2);

                String[] tokens = value.split(",");
                invertedIndex.put(key, tokens);
                line = br.readLine();
            }
        }

        String searchQuery = "Cycling";
        String[] tokens = invertedIndex.get(searchQuery);
        if (tokens != null) {
            for (String token : tokens) {
                System.out.println(Arrays.toString(token.split("#")));
                System.out.println(token.split("#")[1]);
            }
        }
    }

}

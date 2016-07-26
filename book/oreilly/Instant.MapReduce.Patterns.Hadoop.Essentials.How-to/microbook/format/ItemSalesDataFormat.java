package microbook.format;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Srinath Perera (hemapani@apache.org)
 */
public class ItemSalesDataFormat extends FileInputFormat<Text, Text>{
    private ItemSalesDataReader saleDataReader = null; 
        
   
    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext attempt) throws IOException,
            InterruptedException {
        saleDataReader = new ItemSalesDataReader();
        saleDataReader.initialize(inputSplit, attempt);
        return saleDataReader;
    }

}
package com.packt.ch3.etl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CLFMapper extends Mapper<Object, Text, Text, Text> {

    private SimpleDateFormat dateFormatter = 
            new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
    private Pattern p = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\w+) (.+?) (.+?)\" (\\d+) (\\d+) \"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"", Pattern.DOTALL);
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String entry = value.toString();
        Matcher m = p.matcher(entry);
        if (!m.matches()) {
            return;
        }
        Date date = null;
        try {
            date = dateFormatter.parse(m.group(4));
        } catch (ParseException ex) {
            return;
        }
        outputKey.set(m.group(1)); //ip
        StringBuilder b = new StringBuilder();
        b.append(date.getTime()); //timestamp
        b.append('\t');
        b.append(m.group(6)); //page
        b.append('\t');
        b.append(m.group(8)); //http status
        b.append('\t');
        b.append(m.group(9)); //bytes
        b.append('\t');
        b.append(m.group(12)); //useragent
        outputValue.set(b.toString());
        context.write(outputKey, outputValue);
    }
   
}

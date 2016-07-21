package org.dataalgorithms.chapB13.client;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
//
import org.apache.log4j.Logger;
//
import org.dataalgorithms.util.InputOutputUtil;

/**
 *  The InputStreamReaderRunnable class captures 
 *  the output streams generated by submitting a 
 *  Spark job (from a Java code) to a Spark Cluster.
 * 
 * 
 *  @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public class InputStreamReaderRunnable implements Runnable {
    
    static final Logger THE_LOGGER = Logger.getLogger(InputStreamReaderRunnable.class);

    private String name = null;
    private BufferedReader reader = null;


    public InputStreamReaderRunnable(InputStream is, String name) {
        this.name = name;
        this.reader = new BufferedReader(new InputStreamReader(is));        
        THE_LOGGER.info("InputStreamReaderRunnable:  name=" + name);
    }

    @Override
    public void run() {
        try {
            String line = reader.readLine();
            while (line != null) {
                THE_LOGGER.info(line);
                line = reader.readLine();
            }
        } 
        catch (Exception e) {
            THE_LOGGER.error("run() failed. for name="+ name, e);
        }
        finally {
            InputOutputUtil.close(reader);
        }
    }
}
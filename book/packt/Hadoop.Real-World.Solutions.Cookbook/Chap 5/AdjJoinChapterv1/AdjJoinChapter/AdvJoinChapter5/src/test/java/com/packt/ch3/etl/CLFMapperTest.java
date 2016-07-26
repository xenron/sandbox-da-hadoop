package com.packt.ch3.etl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class CLFMapperTest 
    extends TestCase
{
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
    //private Pattern p = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d+) (\\d+) \"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"", Pattern.DOTALL);
    private Pattern p = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\w+) (.+?) (.+?)\" (\\d+) (\\d+) \"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"", Pattern.DOTALL);

    private String sampleEntry = "206.27.154.156 - - [04/Jul/2012:11:51:06 +0000] \"GET /about.html HTTP/1.1\" 200 140 \"http://www.notarealpage.com\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201\"";

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CLFMapperTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( CLFMapperTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
       Matcher m = p.matcher(sampleEntry);
       m.matches();
       assertEquals(m.groupCount(), 13);
       for(int i=1; i < m.groupCount(); i++) {
           if (i == 4) {
                try {
                    Date date = dateFormatter.parse(m.group(i));
                    System.out.println(date);
                } catch (ParseException ex) {
                    Logger.getLogger(CLFMapperTest.class.getName()).log(Level.SEVERE, null, ex);
                    assertTrue(false);
                }
           }
           System.out.println(i + ": " + m.group(i));
       }
    }
}

package com.packt.ch3.etl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/*
 * Not necessary to include with book source code
 */
public class ApacheCLFGenerator 
{
    static Random r = new Random(System.currentTimeMillis());
    static String[] pages = {"GET /index.html HTTP/1.1", "GET /about.html HTTP/1.1", "GET /cart.html HTTP/1.1"};
    static String googleBot = "Mozilla/5.0 (compatible; somebot/9.1; +http://www.somebot.com/robots.html)";
    static String realUser = "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201";
    /*
     * 216.67.1.91 - - [01/Jul/2002:12:11:52 +0000] "GET /index.html HTTP/1.1" 200 431 "http://www.loganalyzer.net/" "Mozilla/4.05 [en] (WinNT; I)"
     */
    static String generateIp() {
        String dottedIp = "";
        dottedIp += Integer.toString(r.nextInt(255)) + ".";
        dottedIp += Integer.toString(r.nextInt(255)) + ".";
        dottedIp += Integer.toString(r.nextInt(255)) + ".";
        dottedIp += Integer.toString(r.nextInt(255));
        return dottedIp;
    }
    static String generateTime() {
        return String.format("%02d:%02d:%02d", (r.nextInt(12) + 1), r.nextInt(59), r.nextInt(59));
    }
    static String generateTimestamp() {
        return String.format("[04/Jul/2012:%s +0000]", generateTime());
    }
    
    static List<String> generateIps(int num, int size) {
        List<String> ips = new ArrayList<String>();
        for(int i=0; i < num; i++) {
            ips.add(generateIp());
        }
        List<String> list = new ArrayList<String>();
        for(int i=0; i < size; i++) {
            list.add(ips.get(i%num));
        }
        return list;
    }
    static List<String> generateUas(int size) {
        List<String> uas = new ArrayList<String>();
        int bots = 0;
        int users = 0;
        for(int i=0; i < size; i++) {
            if(r.nextInt() % 2 == 0) {
                uas.add(realUser);
                users++;
            }
            else {
                uas.add(googleBot);
                bots++;
            }
        }
        System.out.println("users = " + users);
        System.out.println("bots = " + bots);
        return uas;
    }
    public static void main( String[] args ) throws IOException {
        FileWriter fwriter = new FileWriter("apache_clf.txt");
        BufferedWriter bwriter = new BufferedWriter(fwriter);
        List<String> ips = generateIps(4, 1000);
        List<String> uas = generateUas(1000);
        for (int i=0; i < 1000; i++) {
            String line = String.format("%s - - %s \"%s\" 200 140 \"http://www.notarealpage.com\" \"%s\"", ips.get(i), generateTimestamp(), pages[i % (r.nextInt(pages.length) + 1)], uas.get(i));
            System.out.println(line);
            bwriter.write(line+"\n");
        }
    }
}

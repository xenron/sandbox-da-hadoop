package dg.hadoop.platform.homework.ch02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class FileSystemHDFSToFile {


    public static void main(String[] args) throws Exception {
        String inputURI = args[0];
        String outputURI = args[1];
        Configuration conf = new Configuration();
        FileSystem hdfsFs = FileSystem.get(URI.create(inputURI), conf);
        InputStream in = null;
        OutputStream out = null;
        try {
            in = hdfsFs.open(new Path(inputURI));
//            in.skip(101);
//            in = new FileInputStream(inputURI);
            out = new FileOutputStream(outputURI);
//            IOUtils.copyBytes(in, out, 20, false);

            byte[] buffer = new byte[4096];

            int read = in.read(buffer);
            if (read != -1) {
                out.write(buffer, 101, 20);
            }
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

//    public byte[] read(File file) throws IOException {
//
//        ByteArrayOutputStream ous = null;
//        InputStream ios = null;
//        try {
//            byte[] buffer = new byte[4096];
//            ous = new ByteArrayOutputStream();
//            ios = new FileInputStream(file);
//            int read = 0;
//            while ((read = ios.read(buffer)) != -1) {
//                ous.write(buffer, 0, read);
//            }
//        }finally {
//            try {
//                if (ous != null)
//                    ous.close();
//            } catch (IOException e) {
//            }
//
//            try {
//                if (ios != null)
//                    ios.close();
//            } catch (IOException e) {
//            }
//        }
//        return ous.toByteArray();
//    }
}

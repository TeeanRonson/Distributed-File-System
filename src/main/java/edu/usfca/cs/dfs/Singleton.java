package edu.usfca.cs.dfs;

import edu.usfca.cs.dfs.StorageNode.NodeBrainHelperMethods;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Singleton {

    public static void main(String args[]) {

        NodeBrainHelperMethods hm = new NodeBrainHelperMethods(2);

//        String FILEPATH = "FileToRead.txt";
//        File newFile = new File(FILEPATH);
//        File zippedFile = new File("FileToRead.zip");
//        File unzipFile = new File("unzipFile.txt");

        String FILEPATH = "howToReadAPaper.pdf";
        File newFile = new File(FILEPATH);
        File zippedFile = new File("howToReadAPaper.zip");
        File unzipFile = new File("unzipFile.pdf");

//        try {
//            System.out.println(new String(readFromFile(FILEPATH, 0, 30)));
//        } catch (IOException ioe) {
//            ioe.printStackTrace();
//        }


//        compressGzip(newFile, zippedFile);
//        decompressGzip(zippedFile, unzipFile);
//
//        System.out.println(hm.getSha1Checksum(zippedFile));
//
//        System.out.println("This decompresses a zipped file: " + decompressGzip(zippedFile).length);
//        System.out.println("This reads an unzipped file: " + comparison(unzipFile).length);
//
//        System.out.println(Arrays.equals(decompressGzip(zippedFile), comparison(unzipFile)));


        String word = "Hello.hello";
        String[] wordArray = word.split("\\.");
        System.out.println(wordArray[0]);
        System.out.println(wordArray[1]);



    }


    /**
     * Reads from the output filePath
     *
     * @param filePath
     * @param position
     * @param size
     * @return
     * @throws IOException
     */
    private static byte[] readFromFile(String filePath, int position, int size) throws IOException {
        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        file.seek(position);
        byte[] bytes = new byte[size];
        file.read(bytes);
        file.close();
        return bytes;
    }


    /**
     * Compress input file into Gzip file
     *
     * @param input
     * @param output
     * @throws IOException
     */
    public static void compressGzip(File input, File output) {

        try (GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(output))) {
            try (FileInputStream in = new FileInputStream(input)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            } catch (FileNotFoundException fnfe) {
                fnfe.printStackTrace();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        } catch (FileNotFoundException fnfe) {
            System.out.println("FileNotFoundCompression");
        } catch (IOException ioe) {
            System.out.println("IO ExceptionCompression");
        }
    }

    /**
     * Decompress input
     *
     * @param input
     * @param output
     * @throws IOException
     */
    public static void decompressGzip(File input, File output) {


        try (GZIPInputStream in = new GZIPInputStream(new FileInputStream(input))) {
            try (FileOutputStream out = new FileOutputStream(output)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            }
        } catch (FileNotFoundException fnfe) {
            System.out.println("FileNotFound");

        } catch (IOException ioe) {
            System.out.println("IO Exception");
        }
    }


    /**
     * Decompress
     *
     * @param output_file
     * @return
     */
    private static byte[] decompressGzip(File output_file) {

        byte[] buffer = new byte[1024];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(output_file))) {
            int data;
            while ((data = gzipInputStream.read(buffer, 0, 1024)) != -1) {
                out.write(buffer, 0, data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }


    private static byte[] comparison(File file) {

        FileInputStream fileInputStream = null;
        byte[] bFile = new byte[(int) file.length()];
        try {
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bFile);
            fileInputStream.close();
            for (int i = 0; i < bFile.length; i++) {
//                System.out.print((char) bFile[i]);
            }
            System.out.println("\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bFile;
    }
}

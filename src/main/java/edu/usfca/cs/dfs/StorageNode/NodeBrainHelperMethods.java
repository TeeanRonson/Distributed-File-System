package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.Sm;
import edu.usfca.cs.dfs.StorageNode.FileRecovery.FileRecoveryRunner;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class NodeBrainHelperMethods {

    private FileOutputStream outStream = null;
    private int selfId;

    public NodeBrainHelperMethods(int selfId) {
        this.selfId = selfId;
    }

    /**
     * Hashing with SHA1
     *
     * @param newFile
     * @return String hashed
     */
    public String getSha1Checksum(File newFile) {

        //Use SHA-1 algorithm
        String shaChecksum = "";
        try {

            MessageDigest shaDigest = MessageDigest.getInstance("SHA-1");
            //SHA-1 checksum
            shaChecksum = getChunkChecksum(shaDigest, newFile);

        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.out.println("IO Exception in getting Sha1 Checksum ");
        } catch (NoSuchAlgorithmException noae) {
            System.out.println("No Such Algorithm Exception in getting Sha1 Checksum");
        }
        return shaChecksum;
    }

    /**
     * Get the checksum of the file
     *
     * @param digest
     * @param file
     * @return
     * @throws IOException
     */
    private String getChunkChecksum(MessageDigest digest, File file) throws IOException {
        //Get file input stream for reading the file content
        FileInputStream fis = new FileInputStream(file);

        //Create byte array to read data in chunks
        byte[] byteArray = new byte[1024];
        int bytesCount = 0;

        //Read file data and update in message digest
        while ((bytesCount = fis.read(byteArray)) != -1) {
            digest.update(byteArray, 0, bytesCount);
        }
        ;

        //close the stream; We don't need it now.
        fis.close();

        //Get the hash's bytes
        byte[] bytes = digest.digest();

        //This bytes[] has bytes in decimal format;
        //Convert it to hexadecimal format
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        //return complete hash
        return sb.toString();
    }

    /**
     * Calculate the entropy of the Chunk
     *
     * @param input
     * @return
     */
    public static double calculateChunkEntropy(byte[] input) {
        if (input.length == 0) {
            return 0.0;
        }

        /* Total up the occurrences of each byte */
        int[] charCounts = new int[256];
        for (byte b : input) {
            charCounts[b & 0xFF]++;
        }

        double entropy = 0.0;
        for (int i = 0; i < 256; ++i) {
            if (charCounts[i] == 0.0) {
                continue;
            }

            double freq = (double) charCounts[i] / input.length;
            entropy -= freq * (Math.log(freq) / Math.log(2));
        }
        return entropy;
    }

    /**
     * Saves the new file_chunk to disk
     * @param
     * @return
     */
    public boolean saveChunkToDisk(Sm.StoreChunk chunk, String directory) {

        String directoryName = directory.concat(String.valueOf(this.selfId) + File.separator + chunk.getFileName());
        System.out.println(directoryName);

        File newFile;

        try {

            File newDirectory = new File(directoryName);
            //Create directory for non existed path.

            if (!newDirectory.exists()) {
                newDirectory.mkdirs();
            }

            newFile = createFile(chunk, directoryName);

            System.out.println("Canonical path: " + newFile.getCanonicalPath());


        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    private File createFile(Sm.StoreChunk chunk, String directoryName) {

        File newFile = null;

        if (!entropyCheck(chunk)) {
            try {

                newFile = new File(directoryName + File.separator + chunk.getChunkName());

                if (newFile.createNewFile()) {
                    System.out.printf("\n2. Successfully created new file, path:%s", newFile.getCanonicalPath());
                } else { //File may already exist
                    System.out.printf("\n2. Unable to create new file - We will overwrite the existing file!");
                }
                outStream = new FileOutputStream(newFile.getAbsoluteFile());
                outStream.write(chunk.getData().toByteArray());
                outStream.close();

                saveToMemory(chunk, newFile);

                System.out.println("\nHASH STORED:" + StorageNodeBrain.getInstance().getChunkNameToCheckSum());

            } catch (IOException ioe) {
                ioe.printStackTrace();
                System.out.println("can't create a new file from Output Stream!");
            }
        } else {
            newFile = compressGzip(chunk.getData().toByteArray(), directoryName,chunk);
        }
        return newFile;
    }

    /**
     * Save a non-compressed file to to memory
     * @param chunk
     * @param newFile
     */
    private void saveToMemory(Sm.StoreChunk chunk, File newFile) {

        String checkSum = getSha1Checksum(newFile);
        HashMap<String, String> chunkNameToCheckSum = StorageNodeBrain.getInstance().getChunkNameToCheckSum();
        HashMap<String, HashSet<String>> fileToChunkNames = StorageNodeBrain.getInstance().getFileToChunkNames();
        HashMap<String, Integer> chunkNameToId = StorageNodeBrain.getInstance().getChunkNameToId();


        //Update chunkNameToCheckSum
        chunkNameToCheckSum.put(chunk.getChunkName(), checkSum);
        //Update File To ChunkNames
        if (fileToChunkNames.containsKey(chunk.getFileName())) {
            fileToChunkNames.get(chunk.getFileName()).add(chunk.getChunkName());
        } else {
            fileToChunkNames.put(chunk.getFileName(), new HashSet<>(Arrays.asList(chunk.getChunkName())));
        }
        //Update ChunkNameToId
        chunkNameToId.put(chunk.getChunkName(), chunk.getChunkId());
    }


    /**
     * Check if we should compress the file
     * @param chunk
     * @return
     */
    private boolean entropyCheck(Sm.StoreChunk chunk) {

        double entropy = calculateChunkEntropy(chunk.getData().toByteArray());
        double maximumCompression = (1 - (entropy / 8) );
        System.out.println("\nMaximum Compression: " + maximumCompression);
        System.out.println("Threshold: " + (0.6) + "\n");

        if (maximumCompression > (0.6)) {
            System.out.println("WE NEED TO ZIP THE FILE!!!! \n");
            return true;
        }
        return false;
    }


    /**
     * Compress data
     * @param data
     * @param directoryName
     * @param chunk
     * @return
     */
    private File compressGzip(byte[] data, String directoryName, Sm.StoreChunk chunk) {

        String fileName = chunk.getChunkName();
        if (!fileName.endsWith(".zip")) {
            fileName += ".zip";
        }

        File output_file = new File(directoryName + File.separator + fileName);

        try(
                FileOutputStream outputStream     = new FileOutputStream(output_file);
                GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)
        ) {
            gzipOutputStream.write(data);

        } catch (IOException e) {
            e.printStackTrace();
        }

        saveToMemory(output_file, fileName, chunk);

        return output_file;
    }

    /**
     * Save a compressed file to Memory
     * @param output_file
     * @param zipped
     * @param chunk
     */
    private void saveToMemory(File output_file, String zipped, Sm.StoreChunk chunk) {

        String checkSum = getSha1Checksum(output_file);
        HashMap<String, String> chunkNameToCheckSum = StorageNodeBrain.getInstance().getChunkNameToCheckSum();
        HashMap<String, HashSet<String>> fileToChunkNames = StorageNodeBrain.getInstance().getFileToChunkNames();
        HashMap<String, Integer> chunkNameToId = StorageNodeBrain.getInstance().getChunkNameToId();
        HashMap<String, String> chunkNameToZipped = StorageNodeBrain.getInstance().getChunkNameToZipped();

        chunkNameToCheckSum.put(zipped, checkSum);
        chunkNameToZipped.put(chunk.getChunkName(), zipped);
        chunkNameToId.put(zipped, chunk.getChunkId());
        if (fileToChunkNames.containsKey(chunk.getFileName())) {
            fileToChunkNames.get(chunk.getFileName()).add(zipped);
        } else {
            fileToChunkNames.put(chunk.getFileName(), new HashSet<>(Arrays.asList(zipped)));
        }
        System.out.println("ZIPPED HASH STORED:" + StorageNodeBrain.getInstance().getChunkNameToCheckSum());


    }


    /**
     * Decompress
     * @param output_file
     * @return
     */
    private byte[] decompressGzip(File output_file) {
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try(GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(output_file))) {
            int data;
            while ((data = gzipInputStream.read(buffer, 0, 1024)) != -1) {
                out.write(buffer, 0, data);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }


    /**
     * Get contents of entire file
     *
     * @param path
     * @param filename
     * @return
     */
    public byte[] readContentIntoByteArray(String path, String filename, String chunkName, String requestChunk, boolean isZipped) {

        System.out.println("READING CONTENT INTO BYTE ARRAY");
        System.out.println("The request chunk is: " + requestChunk);
        String directoryName = path.concat(String.valueOf(this.selfId) + File.separator + filename);

        //This File could be a zipped or unzipped file - it is what is requested from above
        File file = new File(directoryName + File.separator + requestChunk);

        //Verify the checksum
        verifyChecksum(file, filename, chunkName, requestChunk);

        //If it is zipped, then we need to unzip it before parsing it
        if (isZipped) {
            System.out.println("We need to decompress the file");
            return decompressGzip(file); //returns byte array
        }


        File returningFile = file;

        try {
            System.out.println("Checking the file path:" + file.getCanonicalPath());
        } catch (IOException ioe) {

        }

        FileInputStream fileInputStream = null;
        byte[] bFile = new byte[(int) returningFile.length()];
        try {
            fileInputStream = new FileInputStream(returningFile);
            fileInputStream.read(bFile);
            fileInputStream.close();
//            System.out.println("Streaming Input: ");
            for (int i = 0; i < bFile.length; i++) {
//                System.out.print((char) bFile[i]);
            }
//            System.out.println("\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bFile;
    }


    /**
     * Verify the checksum and fetch the file if it doesnt match up
     * @param file
     * @param filename
     * @param chunkName
     * @param requestChunk
     * @param
     *
     **/
    private void verifyChecksum(File file, String filename, String chunkName, String requestChunk) {

            System.out.println("Verifying the checksum");
            //We have chunkname and requestChunk here because
            //request chunk could be a zip file
            //It is always safe to send the original 'chunkName' to the controller in case the checksum does not match

            String toCompare = getSha1Checksum(file);
            String checkSum = StorageNodeBrain.getInstance().getChunkNameToCheckSum().get(requestChunk);

            if (!toCompare.equals(checkSum)) {
                System.out.println("\n\nChecksum does not match!!!\n\n");

                String controllerHost = StorageNodeBrain.getInstance().getControllerHost();
                int controllerPort = StorageNodeBrain.getInstance().getControllerPort();

                FileRecoveryRunner runner = new FileRecoveryRunner();
                Sm.StoreChunk chunk = runner.replaceFaultyFile(filename, chunkName, controllerHost, controllerPort);

                saveChunkToDisk(chunk, StorageNodeBrain.getInstance().getDirectory());
            }
    }
}

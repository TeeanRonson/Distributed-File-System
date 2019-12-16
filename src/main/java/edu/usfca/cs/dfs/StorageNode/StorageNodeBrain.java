package edu.usfca.cs.dfs.StorageNode;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CONSTANTS;
import edu.usfca.cs.dfs.Sm;
import edu.usfca.cs.dfs.StorageNode.HeartBeat.HeartBeat;
import edu.usfca.cs.dfs.StorageNode.RelayClient.Relay;
import io.netty.channel.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageNodeBrain {

    private static StorageNodeBrain single_instance = null;
                    //fileName -> List<Chunknames>
    private String CONTROLLER;
    private int CONTROLLERPORT;
    private int selfPort;
    private int totalProcessed;
    private int selfId;
    private String selfHost;
    private String directory;
    private ChannelHandlerContext ctx;
    private ReentrantReadWriteLock rwl;

    private NodeBrainHelperMethods helperMethods;

    private HashMap<String, HashSet<String>> fileToChunkNames;
    private HashMap<String, Integer> chunkNameToId;
    private HashMap<String, String> chunkNameToCheckSum;
    private HashMap<String, String> chunkNameToZipped;


    /**
     *
     * Option 2 - on Request
     * 1. When a client requests for a new file
     * 2. Do a checksum check before returning the file to client
     * 3. If checksum does not match
     * 4. Send a request to controller and ask for a list of nodes on where to fetch
     * 5. Ask nodes for the file
     * 6. Save the file and return to client
     */
    private int adding = 0;
    private int test = 0;

    private StorageNodeBrain(String fileName) {
        readConfigurationFile(fileName);
        this.totalProcessed = 0;
        this.fileToChunkNames = new HashMap<>();
        this.rwl = new ReentrantReadWriteLock();
        this.chunkNameToId = new HashMap<>();
        this.chunkNameToCheckSum = new HashMap<>();
        this.chunkNameToZipped = new HashMap<>();
        this.helperMethods = new NodeBrainHelperMethods(this.getSelfId());
    }

    /**
     * Singleton
     * @return
     */
    public static StorageNodeBrain getInstance(String filename) {
        if (single_instance == null) {
            single_instance = new StorageNodeBrain(filename);
        }
        return single_instance;
    }

    public static StorageNodeBrain getInstance() {

//        System.out.println("Is this null? " + single_instance);
        return single_instance;
    }


    /**
     * Read Config file
     * @param fileName
     * @return
     */
    public void readConfigurationFile(String fileName) {

        System.out.println(fileName);
        Gson gson = new Gson();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

            String currentLine;
            JsonObject object;

            while ((currentLine = reader.readLine()) != null) {

                System.out.println(currentLine);
                try {
                    object = gson.fromJson(currentLine, JsonObject.class);


                } catch (JsonSyntaxException e) {
                    e.getMessage();
                    System.out.println("Catching Json Syntax Exception");
                    continue;
                }

                this.CONTROLLERPORT = object.get(CONSTANTS.CONTROLLERPORT).getAsInt();
                this.selfPort = object.get(CONSTANTS.SELFPORT).getAsInt();
                this.selfHost = object.get(CONSTANTS.SELFHOST).getAsString();
                this.CONTROLLER = object.get(CONSTANTS.CONTROLLERHOST).getAsString();
                this.selfId = object.get(CONSTANTS.NODEID).getAsInt();
                this.directory = object.get(CONSTANTS.DIRECTORY).getAsString();
            }

        } catch(IOException e){
            System.out.println("QA File Input Unsuccessful");
            System.exit(0);
        }
    }

    public String getControllerHost() {
        return this.CONTROLLER;
    }

    public int getControllerPort() {
        return this.CONTROLLERPORT;
    }

    public int getSelfPort() {
        return this.selfPort;
    }

    public int getTotalProcessed() {
        return this.totalProcessed;
    }

    public int getSelfId() {
        return this.selfId;
    }

    public String getDirectory() {
        return this.directory;
    }

    public String getSelfHost() {
        return this.selfHost;
    }

    public HashMap<String, String> getChunkNameToCheckSum() {
        return chunkNameToCheckSum;
    }

    public ReentrantReadWriteLock getRwl() {
        return rwl;
    }

    public HashMap<String, String> getChunkNameToZipped() {
        return chunkNameToZipped;
    }

    public HashMap<String, HashSet<String>> getFileToChunkNames() {
        return fileToChunkNames;
    }

    public HashMap<String, Integer> getChunkNameToId() {
        return chunkNameToId;
    }

    /**
     * Run HeartBeats on another Thread
     */
    public boolean runHeartBeats() {

        HeartBeat beat = new HeartBeat();
        beat.start();
        Thread object = new Thread(beat);
        object.start();
        return true;
    }

    public double getAvailableSpace() {

        NumberFormat nf = NumberFormat.getNumberInstance();
        double space = 0;
        for (Path root : FileSystems.getDefault().getRootDirectories()) {

            System.out.print(root + ": ");
            try {
                FileStore store = Files.getFileStore(root);
                space = store.getUsableSpace();
                String asString = nf.format(space);
                System.out.println("available= " + asString
                        + ", total=" + nf.format(store.getTotalSpace()));
            } catch (IOException e) {
                System.out.println("error querying space: " + e.toString());
            }
        }
        return space;
    }

    /**
     * Respond to the Request
     * @param msgWrapper
     */
    private void sendResponse(Sm.StorageMessageWrapper msgWrapper) {

//        System.out.println("STORAGE NODE RESPONDING HERE");

        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
//                    System.out.println("Success");
                }
            }
        });
    }


    /**
     * Store chunk [File name, Chunk Number, Chunk Data]
     * Get number of chunks [File name]
     * Get chunk location [File name, Chunk Number]
     * Retrieve chunk [File name, Chunk Number]
     * List chunks and file names [No input]
     * @param ctx
     * @param msgWrapper
     */
    public void handleIncomingRequest(ChannelHandlerContext ctx, Sm.StorageMessageWrapper msgWrapper) {
        this.ctx = ctx;

        if (msgWrapper != null) {
            this.totalProcessed += 1;
            if (msgWrapper.hasScRequest()) {
                System.out.println("\nSTORE A NEW CHUNK REQUEST");
                Sm.StoreChunkRequest request = msgWrapper.getScRequest();
                storeNewChunkRequest(request);
            } else if (msgWrapper.hasGetNumberOfChunks()) {
                Sm.GetNumberOfChunks request = msgWrapper.getGetNumberOfChunks();
                getNumberOfChunksRequest(request);
            } else if (msgWrapper.hasGetChunkLocation()) {
                Sm.GetChunkLocation request = msgWrapper.getGetChunkLocation();
                getChunkLocationRequest(request);
            } else if (msgWrapper.hasRetrieveChunk()) {
                Sm.RetrieveChunk request = msgWrapper.getRetrieveChunk();
                retrieveChunkRequest(request);
            } else if (msgWrapper.hasListChunksAndFileNames()) {
                listChunksAndFilenames();
            }
        }
    }

    /**
     * Returns everything?
     * List chunks and file names [No input]
     *
     * message RetrieveChunks {
     * repeated RetrieveChunkResponse retrieveChunks = 1;
     *  }
     *  message ListChunksAndFilenamesResponse {
     *  map<string, RetrieveChunks> fileNameToChunks = 1;
     *  }
     *  //for each filename in filesToChunkNames
     *  //for each chunk in chunks
     *  //get file from storage
     *  //add filename and ArrayList to the map
     */
    private void listChunksAndFilenames() {

        HashMap<String, Sm.ChunkNames> toReturn = new HashMap<>();

        try {
            rwl.readLock().lock();
            for (Map.Entry<String, HashSet<String>> entry: fileToChunkNames.entrySet()) {
                ArrayList<String> list = new ArrayList<>(entry.getValue());
                Sm.ChunkNames chunkNames = Sm.ChunkNames.newBuilder()
                        .addAllChunkNames(list)
                        .build();
                toReturn.put(entry.getKey(), chunkNames);
            }

            Sm.ListChunksAndFilenamesResponse response = Sm.ListChunksAndFilenamesResponse.newBuilder()
                    .putAllFileNameToChunks(toReturn)
                    .build();

            //Send the response
            Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                    .setListAllChunksAndFilenames(response)
                    .build();
            sendResponse(msgWrapper);

        } finally {
            rwl.readLock().unlock();
        }
    }

    /**
     * Retrieve chunk [File name, Chunk Number]
     * @param request
     */
    private void retrieveChunkRequest(Sm.RetrieveChunk request) {

        rwl.writeLock().lock();
        System.out.println("\nRETRIEVE CHUNK REQUEST " + adding++);
        rwl.writeLock().unlock();

        try {
            rwl.writeLock().lock();

            String requestChunk = request.getChunkName();
            boolean isZipped = false;

            //If the file has been zipped, then we need to convert the request chunkName into its zipped equivalent
            if (chunkNameToZipped.containsKey(requestChunk)) {
                System.out.println("\nIt's a zipped file! \n");
                requestChunk = chunkNameToZipped.get(requestChunk);
                System.out.println(requestChunk);
                isZipped = true;
            }

            //Get the filename of the chunk if the request doesnt have the filename
            //Why wouldnt it have the filename? Because the controller may be requesting the file instead of the client
            //See HeartBeatPatrol
            String filename = checkFileInMemory(requestChunk);
            System.out.println("The filename is: " + filename);

            //Node does not have the file, so return nothing
            if (filename == null) {
                Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                        .build();
                sendResponse(msgWrapper);
                return;
            }

            System.out.println("\nFind the file\n" + requestChunk);

            for (String chunkName: listFilesUsingJavaIO(filename)) {
                if (requestChunk.equals(chunkName)) {
                    System.out.println("Are we fetching a zipped file: " + isZipped);
                    byte[] payload = helperMethods.readContentIntoByteArray(this.directory, filename, request.getChunkName(), chunkName, isZipped);

//                    System.out.println("Here: " + new String(payload));

                    ByteString data = ByteString.copyFrom(payload);
                    Sm.RetrieveChunkResponse response = Sm.RetrieveChunkResponse.newBuilder()
                            .setFilename(filename)
                            .setChunkName(chunkName)
                            .setChunkSize(payload.length)
                            .setChunkId(chunkNameToId.get(chunkName))
                            .setData(data)
                            .build();
                    Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                            .setRetrieveChunkResponse(response)
                            .build();
                    sendResponse(msgWrapper);
                    System.out.println("-------------------------------------------------------FOUND ONE: " + test++);
                    break;
                }
            }
        } finally {
            rwl.writeLock().unlock();
        }

    }

    /**
     * Checks if we have the Filename of the requested chunk
     * @param requestChunk
     * @return
     */
    private String checkFileInMemory(String requestChunk) {

        System.out.println("FileToChunkNamesSize: " + fileToChunkNames.keySet().size());

        for (Map.Entry<String, HashSet<String>> entry: fileToChunkNames.entrySet()) {
            if (entry.getValue().contains(requestChunk)) {
                return entry.getKey();
            }
        }
        return null;
    }



    /**
     * TODO: What is this?
     * Get chunk location [File name, Chunk Number]
     * @param request
     */
    private void getChunkLocationRequest(Sm.GetChunkLocation request) {

        //Get nodes where the chunk is located

    }

    /**
     * Get number of chunks [File name]
     * @param request
     */
    private void getNumberOfChunksRequest(Sm.GetNumberOfChunks request) {

        try {
            rwl.readLock().lock();
            boolean success = false;
            String filename = request.getFileName();
            int total = 0;
            if (fileToChunkNames.get(filename) != null) {
                success = true;
                total = fileToChunkNames.get(filename).size();
            }

            Sm.GetNumberOfChunksResponse response = Sm.GetNumberOfChunksResponse.newBuilder()
                    .setTotalChunks(total)
                    .setSuccess(success)
                    .build();

            Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                    .setGetNumberOfChunksResponse(response)
                    .build();
            sendResponse(msgWrapper);
        } finally {
            rwl.readLock().unlock();
        }
    }



    /**
     * List out all the files in the directory
     * @param
     * @return
     */
    public Set<String> listFilesUsingJavaIO(String filename) {

        String directoryName = directory.concat(String.valueOf(this.selfId) + File.separator + filename);

        return Stream.of(new File(directoryName).listFiles())
                .filter(file -> !file.isDirectory())
                .map(File::getName)
                .collect(Collectors.toSet());
    }

    /**
     * After receiving a storage request, storage nodes should calculate the
     * Shannon Entropy of the files. If the entropy is less than 0.6
     * (entropy bits / 8), then the chunk should be compressed before it is
     * written to disk.
     *
     * //Create a new list without my node
     * //Do a checksum on the received data
     * //Write data into disk
     * //Save metadata in fileToChunkNames
     * //If there are still nodes in the list
     * //relay to the next node
     * //send response to back to the client that sent it
     */
    private void storeNewChunkRequest(Sm.StoreChunkRequest request) {

        try {

            rwl.writeLock().lock();

            Sm.StoreChunk chunk = request.getChunk();
            List<Sm.StorageNode> nodes = request.getNodes().getNodeListList();
            List<Sm.StorageNode> newSet = new ArrayList<>();

            for (Sm.StorageNode node: nodes) {
                if (node.getId() != this.selfId) {
                    newSet.add(node);
                }
            }
            boolean success = helperMethods.saveChunkToDisk(chunk, this.directory);

            if (newSet.size() > 0) {
                relayToNextNode(chunk, newSet);
            }
            prepareResponse(success);

        } finally {
            rwl.writeLock().unlock();
        }

    }


    /**
     * Store a new chunk response
     * @param success
     */
    private void prepareResponse(boolean success) {

        Sm.StoreChunkResponse response = Sm.StoreChunkResponse.newBuilder()
                .setSuccess(success)
                .build();

        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder().
                setScResponse(response)
                .build();

        sendResponse(msgWrapper);
    }



    /**
     * Relay Chunk to next node
     * @param chunk
     * @param nodes
     * @return
     */
    private void relayToNextNode(Sm.StoreChunk chunk, List<Sm.StorageNode> nodes) {
        Relay relay = new Relay();
        //Prepare remaining nodes and send to next node in pipeline fashion
        String host = nodes.get(0).getIp();
        int port = nodes.get(0).getPort();
        Sm.StorageNodes storageNodes = Sm.StorageNodes.newBuilder()
                .addAllNodeList(nodes)
                .build();

        relay.sendData(chunk, storageNodes, host, port);
    }



}

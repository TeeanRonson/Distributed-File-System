package edu.usfca.cs.dfs.Client;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CONSTANTS;
import edu.usfca.cs.dfs.Client.Objects.FileInfoObject;
import edu.usfca.cs.dfs.Client.Runners.FileRunner;
import edu.usfca.cs.dfs.Sm;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {

    private static Client single_instance = null;
//    private EventLoopGroup workerGroup;
//    private ClientMessagePipeline pipeline;
//    private Bootstrap bootstrap;
    private String controller;
    private int controllerPort;
    private HashMap<String, Integer> filenameToChunkSize;
    private ReentrantReadWriteLock rwl;


    private Client(String configuration) {
        readConfigurationFile(configuration);
//        this.workerGroup = new NioEventLoopGroup();
//        this.pipeline = new ClientMessagePipeline();
//        this.bootstrap = createBootStrap();
        this.filenameToChunkSize = new HashMap<>();
        this.rwl = new ReentrantReadWriteLock();
    }

    /**
     * Constructor with Configuration file
     * @param configuration
     * @return
     */
    public static Client getInstance(String configuration) {

        if (single_instance == null) {
            single_instance = new Client(configuration);
            System.out.println(single_instance);
        }
        return single_instance;
    }

    /**
     * Constructor with no input
     * @return
     */
    public static Client getInstance() {

        System.out.println("Is this null? :" + single_instance);
        return single_instance;
    }
    /**
     * Create the Bootstrap Netty Connection
     * @return
     */
//    private Bootstrap createBootStrap() {
//
//        return new Bootstrap()
//                .group(workerGroup)
//                .channel(NioSocketChannel.class)
//                .option(ChannelOption.SO_KEEPALIVE, true)
//                .handler(pipeline);
//    }

    /**
     *
     * Read the configuration file for bloomSize and totalHashes
     */
    private void readConfigurationFile(String configuration) {

        System.out.println(configuration);
        Gson gson = new Gson();

        try (BufferedReader reader = new BufferedReader(new FileReader(configuration))) {

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

                this.controller = object.get(CONSTANTS.CONTROLLERHOST).getAsString();
                this.controllerPort = object.get(CONSTANTS.CONTROLLERPORT).getAsInt();
            }

        } catch(IOException e){
            System.out.println("QA File Input Unsuccessful");
            System.exit(0);
        }
    }


    public String getController() {
        return this.controller;
    }

    public int getControllerPort() {
        return this.controllerPort;
    }

    /**
     * Send an AssignStorageNodesRequest to the ControllerDriver and receive
     * a response with a map of chunk -> StorageNodes
     * @param request
     */
    private Map<String, Sm.StorageNodes> sendAssignStorageNodesRequest(Sm.AssignStorageNodesRequest request) {

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ClientMessagePipeline pipeline = new ClientMessagePipeline();
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        ChannelFuture cf = bootstrap.connect(this.controller, controllerPort);
        cf.syncUninterruptibly();

        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder().setAsnRequest(request).build();

        Channel chan = cf.channel();
        ClientInboundHandler handler = chan.pipeline().get(ClientInboundHandler.class);
        Sm.StorageMessageWrapper response = handler.request(msgWrapper);

        System.out.println("Got a response for Assigning Nodes: " + response.getAsnResponse().getFileName());
        System.out.println(response.getAsnResponse().getChunksToReplicasMap());

        workerGroup.shutdownGracefully();

        return response.getAsnResponse().getChunksToReplicasMap();
    }

    /**
     * Send a Storage Request to the Primary Storage Node
     *
     * for each chunkName in map.keyset()
     *      send the associated StoreChunk to the primary Storage Node with the replicas list
     * @param chunkToReplicas
     */
    private boolean sendStorageRequest(Map<String, Sm.StorageNodes> chunkToReplicas, ArrayList<Sm.StoreChunk> chunks) {

        int successes = 0;

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ClientMessagePipeline pipeline = new ClientMessagePipeline();
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        for (Sm.StoreChunk sc: chunks) {
            Sm.StorageNodes nodes = chunkToReplicas.get(sc.getChunkName());
            Sm.StorageNode primary = nodes.getNodeList(0);

            System.out.println("HEREEEEEEEEEEEEEEEE");
            System.out.println(primary.getIp());
            System.out.println(primary.getPort());

            //Connect to the first replica
            ChannelFuture cf = bootstrap.connect(primary.getIp(), primary.getPort());
            cf.syncUninterruptibly();

            //Prepare Request & Wrap
            Sm.StoreChunkRequest request = Sm.StoreChunkRequest.newBuilder()
                    .setChunk(sc)
                    .setNodes(nodes)
                    .build();

            Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                    .setScRequest(request)
                    .build();

            //Send
            Channel chan = cf.channel();
            ClientInboundHandler handler = chan.pipeline().get(ClientInboundHandler.class);
            Sm.StorageMessageWrapper response = handler.request(msgWrapper);

            System.out.println("\nDid we get a response from SN?: "+ response);

            if (response.getScResponse().getSuccess()) {
                System.out.println("StoreChunk successful?: " + response.getScResponse().getSuccess());
                successes++;
            }
        }
        workerGroup.shutdownGracefully();

        /* Don't quit until we've disconnected: */
        System.out.println("Total Chunks sent: " + chunks.size());
        System.out.println("Total successes: " + successes);

        return successes == chunks.size();
    }


    /**
     * Sends a RetrieveFileLocation Request to the ControllerDriver
     * @param fileInfo
     * @return
     */
    private Sm.RetrieveFileLocationResponse sendRetrieveFileLocationRequest(FileInfoObject fileInfo) {

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ClientMessagePipeline pipeline = new ClientMessagePipeline();
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        ChannelFuture cf = bootstrap.connect(this.controller, controllerPort);
        cf.syncUninterruptibly();

        Sm.RetrieveFileLocationRequest request = Sm.RetrieveFileLocationRequest.newBuilder()
                .setFileName(fileInfo.getFilename())
                .addAllChunks(fileInfo.getFileChunks())
                .build();

        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                .setRflRequest(request)
                .build();

        Channel chan = cf.channel();
        ClientInboundHandler handler = chan.pipeline().get(ClientInboundHandler.class);
        Sm.StorageMessageWrapper response = handler.request(msgWrapper);

        System.out.println("Got the Response: " + response.getRflResponse().getChunksToReplicasMap().size());
        workerGroup.shutdownGracefully();


        return response.getRflResponse();
    }


    /**
     * Uses Multithreading to get the files from the Storage Nodes
     */
    private boolean getFilesFromStorage(Sm.RetrieveFileLocationResponse response) {


        String filename = response.getFileName();
        Map<String, Sm.StorageNodes> mapping = response.getChunksToReplicasMap();

        System.out.println("----------------------------------------------------------Size of the mapping: " + mapping.size());
        ExecutorService eService = Executors.newFixedThreadPool(15);
        Set<Sm.RetrieveChunkResponse> set = new HashSet<>();

        int i = 0;
        try {
            rwl.writeLock().lock();

            for (Map.Entry<String, Sm.StorageNodes> map: mapping.entrySet()) {
                System.out.println(map.getKey() + " = " + i++);
                Callable<Sm.RetrieveChunkResponse> callable = new FileRunner(filename, map.getKey(), map.getValue());
                Future<Sm.RetrieveChunkResponse> future = eService.submit(callable);
                set.add(future.get());
            }


        } catch (InterruptedException ie) {
            System.out.println("Problem in Threads fetching data from Storage");
        } catch (ExecutionException ee) {
            ee.printStackTrace();
            System.out.println("Execution exception");

        } finally {
            try {
                eService.shutdown();
                eService.awaitTermination(18, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {

            }

            rwl.writeLock().unlock();
        }
        return reconstruct(filename, set);

    }

    /**
     * Reconstructs the file
     * @param set
     */
    private boolean reconstruct(String filename, Set<Sm.RetrieveChunkResponse> set) {

        String FILEPATH = "myFile.txt";
        String pdf = "pdfTest.pdf";
        String atlantic = "atlantic.pdf";
        String resume = "resumeRecreate.pdf";
        String test1 = "Test1Recreate.pdf";

//        System.out.println("\nRECONSTRUCTING!! " + set.size());
        try {

            rwl.readLock().lock();
            int size = filenameToChunkSize.get(filename);
            rwl.readLock().unlock();
//            System.out.println("How many do we have?: " + set.size());
//            System.out.println("Size of each chunk is: " + size);

            //TODO: Uncomment for production
            for (Sm.RetrieveChunkResponse f : set) {
                System.out.println(f.getChunkName() + " " + f.getChunkId() + " " + " " + f.getChunkSize() + " " + (f.getChunkId()) * filenameToChunkSize.get(filename));
                writeToFile("Retrieved_" + filename, f.getData().toByteArray(), f.getChunkId() * size);
            }

            } catch (IOException e) {
            e.printStackTrace();

        }

        return true;
    }

    /**
     * Reads from the output filePath
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
     * Writes to the filePath
     * @param filePath
     * @param data
     * @param position
     * @throws IOException
     */
    private static void writeToFile(String filePath, byte[] data, int position) throws IOException {
        RandomAccessFile file = new RandomAccessFile(filePath, "rw");
        file.seek(position);
        file.write(data);
        file.close();
    }



    /**
     * Break up the file into filechunks
     * Create a AssignStorageNodesRequest and return it
     * Creates the ArrayList<StoreChunks> to be sent to the Storage Nodes later
     * Creates an ArrayList<String> - names of the fileChunks
     * @param fileName
     * @param chunkSize
     * @return
     */
    private FileInfoObject getFileChunks(String fileName, int chunkSize) {

        ArrayList<Sm.StoreChunk> storeChunks = new ArrayList<>();
        ArrayList<Sm.ChunkInfo> fileChunks = new ArrayList<>();

        int count;
        int chunkId = 0;
        int fileSize = 0;
        try (InputStream streamer = new FileInputStream(new File(fileName))){

            System.out.println("Total file size: " + streamer.available());
            fileSize = streamer.available();

            byte[] readSize = new byte[chunkSize];
            while((count = streamer.read(readSize)) != -1) {
                ByteString data = ByteString.copyFrom(readSize, 0, count);
                String[] wordArray = fileName.split("\\.");
                Sm.StoreChunk storeChunkMsg = Sm.StoreChunk.newBuilder()
                        .setFileName(fileName)
                        .setChunkId(chunkId)
                        .setChunkName(wordArray[0] + "_" + chunkId)
                        .setChunkSize(count)
                        .setData(data)
                        .build();
                storeChunks.add(storeChunkMsg);
                Sm.ChunkInfo chunkInfo = Sm.ChunkInfo.newBuilder()
                        .setChunkName(wordArray[0] + "_" + chunkId)
                        .setChunkId(chunkId)
                        .setChunkSize(count)
                        .build();
                fileChunks.add(chunkInfo);
                chunkId++;
            }

            System.out.println("total chunkIds: " + chunkId);

            streamer.close();

        } catch (IOException ioe) {
            System.out.println("Exception when Reading file");
            ioe.printStackTrace();
        }
        return ClientHelperMethods.generateFileInfoObject(storeChunks, fileName, fileSize, fileChunks);
    }


    /**
     * Method first speaks to the controller
     * Then speaks to the Storage Nodes to store the file
     * @param
     */
    public boolean storeNewFile(String filename, int chunkSize) {

        rwl.writeLock().lock();
        filenameToChunkSize.put(filename, chunkSize);
        rwl.writeLock().unlock();

        FileInfoObject fio = getFileChunks(filename, chunkSize);
        Map<String, Sm.StorageNodes> chunkToReplicas = sendAssignStorageNodesRequest(fio.getRequest());

        boolean success = sendStorageRequest(chunkToReplicas, fio.getStoreChunks());
        return success;
    }


    /**
     * Retrieve a file
     * @param filename
     * @return
     */
    public boolean retrieveFile(String filename) {

        System.out.println("RetrieveFile: " + filename);

        if (filenameToChunkSize.keySet().contains(filename)) {
            System.out.println("We have that file");

            //With this approach only one client will know if we have the file previously stored
            FileInfoObject fio = getFileChunks(filename, filenameToChunkSize.get(filename));
            Sm.RetrieveFileLocationResponse response  = sendRetrieveFileLocationRequest(fio);

            System.out.println("\nWe know where to get them. Now we fetch from Storage Nodes.");
//            System.out.println(response.getChunksToReplicasMap());
            System.out.println(getFilesFromStorage(response));

            return true;

        } else {
            //Let user know that we do not have the given file
            System.out.println("We do not have that file!");
            return false;
        }

    }


    /**
     * The client will also be able to print out a list of active nodes (retrieved from the controller),
     * the total disk space available in the cluster (in GB), and number of requests handled by each node.
     * @return
     */
    public Map<Integer, Sm.HeartBeat> getAllStorageNodeData() {


        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ClientMessagePipeline pipeline = new ClientMessagePipeline();
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        ChannelFuture cf = bootstrap.connect(this.controller, controllerPort);
        cf.syncUninterruptibly();

        Sm.GetActiveNodes request = Sm.GetActiveNodes.newBuilder()
                .build();

        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                .setActiveNodes(request)
                .build();

        Channel chan = cf.channel();
        ClientInboundHandler handler = chan.pipeline().get(ClientInboundHandler.class);
        Sm.StorageMessageWrapper response = handler.request(msgWrapper);

        System.out.println("Got the Response: " + response.getActiveNodes());
        workerGroup.shutdownGracefully();

        return response.getActiveNodes().getActiveNodesMap();

    }



}

































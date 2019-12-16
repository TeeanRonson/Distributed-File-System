package edu.usfca.cs.dfs.Controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import edu.usfca.cs.dfs.CONSTANTS;
import edu.usfca.cs.dfs.Controller.HeatBeatPatrol.HeartBeatPatrol;
import edu.usfca.cs.dfs.Controller.HeatBeatPatrol.PatrolInboundHandler;
import edu.usfca.cs.dfs.Controller.HeatBeatPatrol.PatrolMessagePipeline;
import edu.usfca.cs.dfs.Sm;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Controller {


    /**
     *
     * 1. Launch a thread to check the nodeTimestamps
     * 2. If a timestamp has not been updated in the last 10 secs
     * 3. Kill the nodes in active nodes
     * 4. Use replicasToChunkNames to check all the chunkNames that the Replica had
     * 5. Send those chunkNames to another nodes that dont have those chunks
     * 6. Done
     *
     */
    private static Controller single_instance = null;
    //NOTE: Data structures for StorageNodes
    //--------------HostName, Timestamps
    private ConcurrentHashMap<Integer, Long> nodeTimestamps;

    //NOTE: Data structures for Client Requests
    //--------------HostName, BloomFilter
    private HashMap<Integer, BloomFilter> routingTable;
    //--------------HostName, Relay
    private HashMap<Integer, Sm.HeartBeat> activeNodes;
    //This is to maintain that if the same file comes in with less or more data, we see it here
    private HashMap<String, Integer> fileToFileSize;
    //If a chunk name already exists, then we should update the chunk at the existing replicas
    private HashMap<String, ArrayList<Sm.StorageNode>> chunkNamesToReplicas;
    //So we know which replicas have which chunkNames - for recovery?
    private HashMap<Integer, HashSet<String>> replicasToChunkNames;
    private int bloomSize;
    private int totalHashes;
    private ChannelHandlerContext ctx;
    private ReentrantReadWriteLock rwL;
    private boolean runPatrol;

    private Controller(String configuration) {
        this.routingTable = new HashMap<>();
        this.nodeTimestamps = new ConcurrentHashMap<>();
        this.activeNodes = new HashMap<>();
        this.fileToFileSize = new HashMap<>();
        this.chunkNamesToReplicas = new HashMap<>();
        this.replicasToChunkNames = new HashMap<>();
        this.runPatrol = false;
        this.rwL = new ReentrantReadWriteLock();
        readConfigurationFile(configuration);
    }



    public static Controller getInstance(String configuration) {

        if (single_instance == null) {
            single_instance = new Controller(configuration);
        }
        return single_instance;
    }

    public static Controller getInstance() {
        return single_instance;
    }

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

                this.bloomSize = object.get(CONSTANTS.BLOOMFILTERSIZE).getAsInt();
                this.totalHashes = object.get(CONSTANTS.HASHES).getAsInt();
            }

        } catch(IOException e){
            System.out.println("QA File Input Unsuccessful");
            System.exit(0);
        }
    }

    public HashMap<Integer, HashSet<String>> getReplicasToChunkNames() {
        return replicasToChunkNames;
    }

    public ConcurrentHashMap<Integer, Long> getNodeTimestamps() {
        return nodeTimestamps;
    }

    public HashMap<Integer, Sm.HeartBeat> getActiveNodes() {
        return activeNodes;
    }

    public HashMap<Integer, BloomFilter> getRoutingTable() {
        return routingTable;
    }

    public HashMap<String, ArrayList<Sm.StorageNode>> getChunkNamesToReplicas() {
        return chunkNamesToReplicas;
    }

    public HashMap<String, Integer> getFileToFileSize() {
        return fileToFileSize;
    }

    public ReentrantReadWriteLock getRwL() {
        return rwL;
    }

    /**
     * Respond to the Request
     * @param msgWrapper
     */
    private void sendResponse(Sm.StorageMessageWrapper msgWrapper) {

        System.out.println("RESPONDING HERE ----------------------");

        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    System.out.println("Success");
                }
            }
        });

    }

    /**
     *
     * @param incomingRequest
     * @return
     */
    public void handleRequest(ChannelHandlerContext ctx, Sm.StorageMessageWrapper incomingRequest) {
        this.ctx = ctx;

        if (incomingRequest != null) {
            if (incomingRequest.hasAsnRequest()) {
                System.out.println("Got a Request to Store a New File ");
                handleAsnRequest(incomingRequest);
            } else if (incomingRequest.hasRflRequest()) {
                System.out.println("Got a Request to Retrieve a New File -----------");
                handleRflRequest(incomingRequest);
            } else if (incomingRequest.hasHeartBeat()) {
                Sm.HeartBeat heartBeat = incomingRequest.getHeartBeat();
                storageNodeHeartBeat(heartBeat);
            } else if (incomingRequest.hasActiveNodes()) {
                getAllActiveNodes();
            } else if (incomingRequest.hasListReplicasAndChunks()) {
//                prepareReplicasToChunkNames();
            }
        }
    }

//    private void prepareReplicasToChunkNames() {
//
//        try {
//            rwL.readLock().lock();
//
//            HashMap<Integer, ArrayList<String>> toReturn = new HashMap<>();
//            for (Map.Entry<Integer, HashSet<String>> map: replicasToChunkNames.entrySet()) {
//                toReturn.put(map.getKey(), Arrays.asList(ma))
//
//            }
//            Sm.ListReplicasAndChunksResponse response = Sm.ListReplicasAndChunksResponse.newBuilder()
//                    .putAllReplicasToChunkNames()
//                    .build();
//
//            Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
//                    .setActiveNodes(response)
//                    .build();
//            sendResponse(msgWrapper);
//
//        } finally {
//            rwL.readLock().unlock();
//        }
//
//
//    }

    /**
     * A method to handle a Assign Storage Node Request
     * @param clientRequest
     */
    private void handleAsnRequest(Sm.StorageMessageWrapper clientRequest) {

        Sm.AssignStorageNodesRequest request = clientRequest.getAsnRequest();

        //Prepare the response
        Sm.AssignStorageNodesResponse response = prepareNodesForChunks(request);

        //Wrap it and return to client
        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                .setAsnResponse(response)
                .build();
        sendResponse(msgWrapper);
    }

    private void handleRflRequest(Sm.StorageMessageWrapper clientRequest) {

        Sm.RetrieveFileLocationRequest request = clientRequest.getRflRequest();

        Sm.RetrieveFileLocationResponse response = prepareNodesForChunksBf(request);
        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                .setRflResponse(response)
                .build();
        sendResponse(msgWrapper);

    }

    /**
     * Check bloomfilters to retrieve possible nodes
     * @param request
     * @return
     */
    private Sm.RetrieveFileLocationResponse prepareNodesForChunksBf(Sm.RetrieveFileLocationRequest request) {


        HashMap<String, Sm.StorageNodes> toReturn = new HashMap<>();

        for (Sm.ChunkInfo chunk: request.getChunksList()) {
            ArrayList<Sm.StorageNode> replicas = getNodesBF(chunk);
            Sm.StorageNodes nodes = Sm.StorageNodes.newBuilder()
                    .addAllNodeList(replicas)
                    .build();
            toReturn.put(chunk.getChunkName(), nodes);
        }

        Sm.RetrieveFileLocationResponse response = Sm.RetrieveFileLocationResponse.newBuilder()
                .setFileName(request.getFileName())
                .putAllChunksToReplicas(toReturn)
                .build();


        System.out.println("\n Lets see whats in the ToReturn: \n" + toReturn);

        return response;
    }

    /**
     * Get 3 nodes randomly for the new chunkName
     * @param chunk
     * @return
     */
    private ArrayList<Sm.StorageNode> getNodesBF(Sm.ChunkInfo chunk) {

        ArrayList<Sm.StorageNode> nodes = new ArrayList<>();

        try {
            rwL.readLock().lock();

            for (Map.Entry<Integer, BloomFilter> entry: routingTable.entrySet()) {
                if (entry.getValue().get(chunk.getChunkName().getBytes())) {
                    Sm.HeartBeat hb = activeNodes.get(entry.getKey());
                    nodes.add(Sm.StorageNode.newBuilder()
                            .setIp(hb.getIp())
                            .setPort(hb.getPort())
                            .setId(hb.getId())
                            .build());
                }
            }
            return nodes;

        } finally {
            rwL.readLock().unlock();
        }
    }

    /**
     * Prepare the Nodes for each Chunk
     *  //Read in the request
     //Update the filename -> filesize
     //For each chunk, find a set of nodes
     //if primary node exists in primaryToReplicas
     //get replicas from primaryToReplicas
     //else
     //find 2 other nodes
     //add chunk ->  nodes into response
     //set bits in their respective bloomFilters
     * @param request
     * @return
     */
    private Sm.AssignStorageNodesResponse prepareNodesForChunks(Sm.AssignStorageNodesRequest request) {

        HashMap<String, Sm.StorageNodes> toReturn = new HashMap<>();
        updateFilenameToFilesize(request.getFileName(), request.getFileSize()); //---- Update

        for (Sm.ChunkInfo chunk: request.getChunksList()) {
            ArrayList<Sm.StorageNode> replicas = getNodes(chunk);
            Sm.StorageNodes nodes = Sm.StorageNodes.newBuilder()
                    .addAllNodeList(replicas)
                    .build();
            toReturn.put(chunk.getChunkName(), nodes);
            updateReplicasAndChunkNames(replicas, chunk.getChunkName());                        //---- Update
        }

        Sm.AssignStorageNodesResponse response = Sm.AssignStorageNodesResponse.newBuilder()
                .setFileName(request.getFileName())
                .setFileSize(request.getFileSize())
                .putAllChunksToReplicas(toReturn)
                .build();

        return response;
    }

    /**
     * Get 3 nodes randomly for the new chunkName
     * @param chunk
     * @return
     *
     *
     * TODO: Random node Id's should be selected based on the id's of the existing nodes, not the size of activeNodes
     *
     */
    private ArrayList<Sm.StorageNode> getNodes(Sm.ChunkInfo chunk) {

        ArrayList<Sm.StorageNode> nodes = new ArrayList<>();
        HashSet<Integer> ids = new HashSet<>();

        try {
            rwL.readLock().lock();

            //if I dont store this, the next time I get a chunk that has the same name, I wont be able to overwrite the existing
            if (chunkNamesToReplicas.containsKey(chunk.getChunkName())) {
                return chunkNamesToReplicas.get(chunk.getChunkName());
            }

//            while (nodes.size() != 3) {
//                int id = generateRandom();
//
//                //count = random + sizeOfActiveNodes % sizeOfActiveNodes
//                //loop through activeNodes until count == 0
//                //get the id at that point and add it to the HashSet
//                if (!ids.contains(id)) {
//                    ids.add(id);
//                    Sm.HeartBeat heartBeat = activeNodes.get(id);
//                    if (heartBeat != null && heartBeat.getFreeSpace() > chunk.getChunkSize()) {
//                        nodes.add(Sm.StorageNode.newBuilder()
//                                .setIp(heartBeat.getIp())
//                                .setPort(heartBeat.getPort())
//                                .setId(heartBeat.getId())
//                                .build());
//                    }
//                }
//            }

            while (nodes.size() != 3) {
//                System.out.println("SIZE:" + nodes.size());
                int value = generateRandom();
                int count = (value + this.activeNodes.size()) % this.activeNodes.size();
//                System.out.println("Count: " + count);
                int id = 0;
                for (int nodeId: activeNodes.keySet()) {
                    if (count == 0) {
                        id = nodeId;
                    }
                    count--;
                }
                if (!ids.contains(id)) {
//                    System.out.println("Selected id: " + id);

                    ids.add(id);
                    Sm.HeartBeat heartBeat = activeNodes.get(id);
                    if (heartBeat != null && heartBeat.getFreeSpace() > chunk.getChunkSize()) {
//                        System.out.println("add id");
                        nodes.add(Sm.StorageNode.newBuilder()
                                .setIp(heartBeat.getIp())
                                .setPort(heartBeat.getPort())
                                .setId(heartBeat.getId())
                                .build());
                    }
                }
            }


            return nodes;
        } finally {
            rwL.readLock().unlock();
        }
    }


    /**
     * Generate Random
     * @return
     */
    private int generateRandom() {

        Random random = new Random();
//        return random.nextInt(activeNodes.size() - 1 + 1) + 1;
        return random.nextInt(12 - 1 + 1) + 1;

    }


    /**
     * Update replicas
     * @param replicas
     * @param chunkName
     */
    public void updateReplicasAndChunkNames(ArrayList<Sm.StorageNode> replicas, String chunkName) {

        try {
            rwL.writeLock().lock();

            for (Sm.StorageNode node: replicas) {
                if(replicasToChunkNames.containsKey(node.getId())) {
                    replicasToChunkNames.get(node.getId()).add(chunkName);
                } else {
                    replicasToChunkNames.put(node.getId(), new HashSet<>(Arrays.asList(chunkName)));
                }

                if (this.routingTable.get(node.getId()) != null) {
                    this.routingTable.get(node.getId()).put(chunkName.getBytes());
                }
            }
            chunkNamesToReplicas.put(chunkName, replicas);

        } finally {
            rwL.writeLock().unlock();
        }
    }

    /**
     * The client will also be able to print out a list of active nodes
     * (retrieved from the controller), the total disk space available in the
     * cluster (in GB), and number of requests handled by each node.
     */
    private void getAllActiveNodes() {

        try {
            rwL.readLock().lock();

            Sm.GetActiveNodes response = Sm.GetActiveNodes.newBuilder()
                    .putAllActiveNodes(activeNodes)
                    .build();

            Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                    .setActiveNodes(response)
                    .build();
            sendResponse(msgWrapper);


            for (Map.Entry<Integer, HashSet<String>> map: replicasToChunkNames.entrySet()) {
                System.out.println(map.getKey() + ": " + map.getValue().size() + " --> " + map.getValue());
            }

        } finally {
            rwL.readLock().unlock();
        }

    }

    /**
     * Filename to filesize
     * @param filename
     * @param filesize
     */
    private void updateFilenameToFilesize(String filename, int filesize) {

        try {
            rwL.writeLock().lock();
            fileToFileSize.put(filename, filesize);
        } finally {
            rwL.writeLock().unlock();
        }
    }


    /**
     * Store a new Relay or update an existing Relay
     *
     * @param heartBeat
     */
    private void storageNodeHeartBeat(Sm.HeartBeat heartBeat) {

        try {
                rwL.writeLock().lock();

                long currTime = System.currentTimeMillis();
                int nodeId = heartBeat.getId();
                System.out.println("The Id of the node is: " + nodeId);

                if (activeNodes.get(nodeId) == null) {
                    System.out.println("New Node");
                    createBloomFilter(nodeId);
                    System.out.println("NEW BLOOM FILTER FOR: " + nodeId);
                }

                activeNodes.put(nodeId, heartBeat);
                nodeTimestamps.put(nodeId, currTime);
                System.out.println("BloomFilterSize: " + this.routingTable.size());
                System.out.println("ActiveNodes: " + this.activeNodes.size());
//                System.out.println("NodeId: " + this.nodeTimestamps.get(nodeId));

                if (activeNodes.size() > 0 && !runPatrol) {
                    runPatrol = true;
                    patrolHeartBeats();
                }


        } finally {
                rwL.writeLock().unlock();
            }
    }

    /**
     * Patrol HeartBeats to monitor failed nodes
     */
    private void patrolHeartBeats() {

        HeartBeatPatrol patrol = new HeartBeatPatrol();
        Thread thread = new Thread(patrol);
        thread.start();
    }

    /**
     * Add a new BloomFilter into the routing Table
     * Doesnt need a Write Lock because it is called from the storageNodeHeartBeat method
     */
    private void createBloomFilter(int nodeId) {
        routingTable.put(nodeId, new BloomFilter(bloomSize, totalHashes));
    }



}

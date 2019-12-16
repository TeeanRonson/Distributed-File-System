package edu.usfca.cs.dfs.Controller.HeatBeatPatrol;

import edu.usfca.cs.dfs.CONSTANTS;
import edu.usfca.cs.dfs.Controller.BloomFilter;
import edu.usfca.cs.dfs.Controller.Controller;
import edu.usfca.cs.dfs.Sm;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HeartBeatPatrol implements Runnable {


    private HashMap<Integer, Sm.HeartBeat> activeNodes;
    private ConcurrentHashMap<Integer, Long> nodeTimestamps;
    private ReentrantReadWriteLock rwL;
    private HashMap<Integer, HashSet<String>> replicasToChunkNames;
    private HashMap<Integer, BloomFilter> routingTable;



    public HeartBeatPatrol() {
        this.activeNodes = Controller.getInstance().getActiveNodes();
        this.nodeTimestamps = Controller.getInstance().getNodeTimestamps();
        this.rwL = Controller.getInstance().getRwL();
        this.replicasToChunkNames = Controller.getInstance().getReplicasToChunkNames();
        this.routingTable = Controller.getInstance().getRoutingTable();

    }

    @Override
    public void run() {

        while(activeNodes.size() >= 0) {

            try {
                TimeUnit.SECONDS.sleep(CONSTANTS.HEARTBEAT_PATROL_SECONDS);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }

            System.out.println("Patrolling Storage Nodes -------------------------------------------------------");
            System.out.println("\nNodes Available:");
            Set<String> chunkSet = null;

                for (Map.Entry<Integer, Long> entry : nodeTimestamps.entrySet()) {

                    System.out.println(entry.getKey());
                    if (entry.getValue() < System.currentTimeMillis() - CONSTANTS.STORAGENODE_DEAD_THRESHOLD) { //20 seconds

                        System.out.println("\nNODE DOWN NODE DOWN NODE DOWN!!!! " + entry.getKey() + "\n");
                        int nodeId = entry.getKey();
                        rwL.readLock().lock();
                        if (replicasToChunkNames.size() != 0) {
                            chunkSet = new HashSet<>(replicasToChunkNames.get(nodeId));
                        }
                        rwL.readLock().unlock();

                        rwL.writeLock().lock();
                        System.out.println("RemovingNode--------------------------------------------------");
                        activeNodes.remove(nodeId);
                        nodeTimestamps.remove(nodeId);
                        routingTable.remove(nodeId);
                        replicasToChunkNames.remove(nodeId);
                        rwL.writeLock().unlock();

                        //Get all the chunks in this dead node and Back them up
                        if (chunkSet != null) {
                            System.out.println(chunkSet);
                            for (String chunk : chunkSet) {
                                System.out.println("Backup chunk: " + chunk);
                                backupChunks(chunk);
                            }
                        }
                    }
                    System.out.println();
                }
        }
    }

//    /**
//     * Check the HeartBeats for deadnodes every 20 secs
//     */
//    public void patrolHeartBeats() {
//
//        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
//
//        executorService.scheduleAtFixedRate(() -> {
//            System.out.println("Nodes Available:");
//            for (Map.Entry<Integer, Long> entry : nodeTimestamps.entrySet()) {
//                if (entry.getValue() < System.currentTimeMillis() - CONSTANTS.STORAGENODE_DEAD_THRESHOLD) { //20 seconds
//
//                    int nodeId = entry.getKey();
//                    rwL.readLock().lock();
//                    Set<String> chunkSet = new HashSet<>(replicasToChunkNames.get(nodeId));
//                    rwL.readLock().unlock();
//
//                    rwL.writeLock().lock();
//                    activeNodes.remove(nodeId);
//                    nodeTimestamps.remove(nodeId);
//                    routingTable.remove(nodeId);
//                    rwL.writeLock().unlock();
//
//                    //Get all the chunks in this dead node and Back them up
//                    for (String chunk: chunkSet) {
//                        System.out.println("Backup chunk: " + chunk);
//                        backupChunks(chunk);
//                    }
//                }
//                System.out.println(entry.getValue());
//            }
//        }, 0, CONSTANTS.HEARTBEAT_PATROL_SECONDS, TimeUnit.SECONDS);
//    }

    /**
     * Back up each chunk by finding where it may be stored except on the failed NodeId
     * @param chunkName
     */
    private void backupChunks(String chunkName) {

        ArrayList<Sm.StorageNode> toRetrieve = new ArrayList<>();
        ArrayList<Sm.StorageNode> toSend = new ArrayList<>();

        //If true, thats where we want to retrieve it
        //If false, thats where we want to send it to
        for (Map.Entry<Integer, BloomFilter> routing: routingTable.entrySet()) {

            Sm.StorageNode node = Sm.StorageNode.newBuilder()
                    .setId(routing.getKey())
                    .setIp(activeNodes.get(routing.getKey()).getIp())
                    .setPort(activeNodes.get(routing.getKey()).getPort())
                    .build();

            //If the bloomfilter returns true
            //retrieve from those nodes
            if (routing.getValue().get(chunkName.getBytes())) {
                toRetrieve.add(node);
            } else {
                //Send it to the nodes that dont have the chunk name
                if (toSend.size() < 3) {
                    toSend.add(node);
                }
            }
        }
        //Checks which nodes have the chunk and which do not
//        for (Map.Entry<Integer, HashSet<String>> map: replicasToChunkNames.entrySet()) {
//            Sm.StorageNode node = Sm.StorageNode.newBuilder()
//                    .setId(map.getKey())
//                    .setIp(activeNodes.get(map.getKey()).getIp())
//                    .setPort(activeNodes.get(map.getKey()).getPort())
//                    .build();
//            if (map.getValue().contains(chunkName)) {
//                toRetrieve.add(node);
//            } else {
//                if (toSend.size() < 3) {
//                    toSend.add(node);
//
//                }
//            }
//        }

        retrieve(toRetrieve, toSend, chunkName);
    }

    private void retrieve(ArrayList<Sm.StorageNode> toRetrieve, ArrayList<Sm.StorageNode> toSend, String chunkName) {

        Sm.RetrieveChunkResponse chunkResponse = null;

        System.out.println("Nodes to retrieve from: " + toRetrieve);

        //Setup

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        PatrolMessagePipeline pipeline = new PatrolMessagePipeline();
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);


        for (Sm.StorageNode node: toRetrieve) {

            //Connect to controller
            ChannelFuture cf = bootstrap.connect(node.getIp(), node.getPort());
            cf.syncUninterruptibly();

            //Build the Retrieve Chunk, does not have the Filename
            Sm.RetrieveChunk request = Sm.RetrieveChunk.newBuilder()
                    .setChunkName(chunkName)
                    .build();


            //Build Wrapper
            Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                    .setRetrieveChunk(request)
                    .build();


            //Send the request
            System.out.println("\n\nFetching New File to Replace Faulty one! " + chunkName + "\n\n");

            Channel chan = cf.channel();
            PatrolInboundHandler handler = chan.pipeline().get(PatrolInboundHandler.class);
            Sm.StorageMessageWrapper response = handler.request(msgWrapper);

            if (response.getRetrieveChunkResponse() != null) {
                System.out.println("Not NULL");
                chunkResponse = response.getRetrieveChunkResponse();
                if (chunkResponse.getChunkName().equals(chunkName)) {
                    System.out.println("We got the file: " + chunkResponse.getChunkName());
                    break;
                }
                System.out.println("The response had this name: " + chunkResponse.getChunkName());
            }
            System.out.println("NULL?");
        }
        workerGroup.shutdownGracefully();

        sendToNodes(chunkResponse, toSend);
    }

    /**
     * Send to nodes that dont have the chunk
     * @param chunkResponse
     * @param toSend
     */
    private void sendToNodes(Sm.RetrieveChunkResponse chunkResponse, ArrayList<Sm.StorageNode> toSend) {

        System.out.println("Nodes to send to: "  + toSend);

        if (toSend.size() != 0) {
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            PatrolMessagePipeline pipeline = new PatrolMessagePipeline();
            Bootstrap bootstrap = new Bootstrap()
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(pipeline);

//        for (Sm.StorageNode node: toSend) {

            //create StoreChunkRequest
            Sm.StoreChunk chunk = Sm.StoreChunk.newBuilder()
                    .setFileName(chunkResponse.getFilename())
                    .setChunkId(chunkResponse.getChunkId())
                    .setChunkSize(chunkResponse.getChunkSize())
                    .setData(chunkResponse.getData())
                    .setChunkName(chunkResponse.getChunkName())
                    .build();

            Sm.StorageNodes nodes = Sm.StorageNodes.newBuilder()
                    .addAllNodeList(toSend)
                    .build();

            Sm.StorageNode primary = toSend.get(0);
            System.out.println("Primary: " + primary.getIp());
            System.out.println("Primary: " + primary.getPort());

            //Connect to the first replica
            ChannelFuture cf = bootstrap.connect(primary.getIp(), primary.getPort());
            cf.syncUninterruptibly();

            //Prepare Request & Wrap
            Sm.StoreChunkRequest request = Sm.StoreChunkRequest.newBuilder()
                    .setChunk(chunk)
                    .setNodes(nodes)
                    .build();

            Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                    .setScRequest(request)
                    .build();

            //Send
            Channel chan = cf.channel();
            PatrolInboundHandler handler = chan.pipeline().get(PatrolInboundHandler.class);
            Sm.StorageMessageWrapper response = handler.request(msgWrapper);

            if (response.getScResponse().getSuccess()) {
                System.out.println("StoreChunk successful?: " + response.getScResponse().getSuccess());
                Controller.getInstance().updateReplicasAndChunkNames(toSend, chunk.getChunkName());
            }

            workerGroup.shutdownGracefully();
        }
    }
}

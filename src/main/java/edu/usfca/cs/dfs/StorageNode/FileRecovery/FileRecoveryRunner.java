package edu.usfca.cs.dfs.StorageNode.FileRecovery;

import edu.usfca.cs.dfs.Sm;
import edu.usfca.cs.dfs.StorageNode.StorageNodeBrain;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.ArrayList;
import java.util.Map;

public class FileRecoveryRunner {


    public FileRecoveryRunner() {


    }

    /**
     * Connect to controller and retrieve possible nodes with chunk
     * @param chunkName
     * @param host
     * @param port
     * @return
     */
    public Sm.StoreChunk replaceFaultyFile(String filename, String chunkName, String host, int port) {

        //Setup
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        FileRecoveryRunnerMessagePipeline pipeline = new FileRecoveryRunnerMessagePipeline();
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        //Connect to controller
        ChannelFuture cf = bootstrap.connect(host, port);
        cf.syncUninterruptibly();

        //Build the chunk info
        Sm.ChunkInfo chunkInfo = Sm.ChunkInfo.newBuilder()
                .setChunkName(chunkName)
                .build();

        ArrayList<Sm.ChunkInfo> chunks = new ArrayList<>();
        chunks.add(chunkInfo);

        //Build StorageChunkRequest
        Sm.RetrieveFileLocationRequest request = Sm.RetrieveFileLocationRequest.newBuilder()
                .setFileName(filename)
                .addAllChunks(chunks)
                .build();

        //Build MsgWrapper
        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                .setRflRequest(request)
                .build();

        //Send the request
        System.out.println("\n\nFetching New File to Replace Faulty one! " + chunkName + "\n\n");

        Channel chan = cf.channel();
        FileRecoveryRunnerInboundHandler handler = chan.pipeline().get(FileRecoveryRunnerInboundHandler.class);
        Sm.StorageMessageWrapper response = handler.request(msgWrapper);
        workerGroup.shutdownGracefully();

        return fetchFromNodes(response.getRflResponse().getChunksToReplicasMap(), filename, chunkName);
    }

    /**
     * Fetch the chunk from the RFLResponse nodes
     * @param mapping
     * @param chunkName
     * @return
     */
    private Sm.StoreChunk fetchFromNodes(Map<String, Sm.StorageNodes> mapping, String filename, String chunkName) {

        System.out.println("Got the nodes to retrieve the Chunk here: " + mapping);

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        FileRecoveryRunnerMessagePipeline pipeline = new FileRecoveryRunnerMessagePipeline();
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        for (Sm.StorageNode node: mapping.get(chunkName).getNodeListList()) {

            //Go to everyone except myself
            if (node.getId() != StorageNodeBrain.getInstance().getSelfId()) {

                System.out.println("Going to fetch at: " + node.getId());

                //Connect to controller
                ChannelFuture cf = bootstrap.connect(node.getIp(), node.getPort());
                cf.syncUninterruptibly();

                //Build a RequestChunk
                Sm.RetrieveChunk request = Sm.RetrieveChunk.newBuilder()
                        .setFilename(filename)
                        .setChunkName(chunkName)
                        .build();

                Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                        .setRetrieveChunk(request)
                        .build();

                Channel chan = cf.channel();
                FileRecoveryRunnerInboundHandler handler = chan.pipeline().get(FileRecoveryRunnerInboundHandler.class);
                Sm.StorageMessageWrapper response = handler.request(msgWrapper);

                if (response.getRetrieveChunkResponse() != null) {
                    Sm.RetrieveChunkResponse retrievedChunk = response.getRetrieveChunkResponse();
                    System.out.println("\n\nFileRecoveryRunner has received a chunk: " + response.getRetrieveChunkResponse().getChunkName());
                    Sm.StoreChunk chunk = Sm.StoreChunk.newBuilder()
                            .setFileName(filename)
                            .setChunkName(retrievedChunk.getChunkName())
                            .setChunkId(retrievedChunk.getChunkId())
                            .setChunkSize(retrievedChunk.getChunkSize())
                            .setData(retrievedChunk.getData())
                            .build();

                    return chunk;
                }
            }
        }

        workerGroup.shutdownGracefully();
        return null;
    }
}




















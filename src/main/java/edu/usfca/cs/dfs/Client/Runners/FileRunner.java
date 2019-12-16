package edu.usfca.cs.dfs.Client.Runners;

import edu.usfca.cs.dfs.Sm;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

//Am I storing the metadata information of chunks in the storage Node?
//i.e. chunk size, chunk name,


public class FileRunner implements Callable<Sm.RetrieveChunkResponse> {

//    private EventLoopGroup workerGroup;
//    private FileRunnerMessagePipeline pipeline;
//    private Bootstrap bootstrap;
    private String filename;
    private String chunkName;
    private Sm.StorageNodes replicas;

    public FileRunner(String filename, String chunkName, Sm.StorageNodes replicas) {
//        this.workerGroup = new NioEventLoopGroup();
//        this.pipeline = new FileRunnerMessagePipeline();
//        this.bootstrap = createBootStrap();
        this.filename = filename;
        this.chunkName = chunkName;
        this.replicas = replicas;
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

    @Override
    public Sm.RetrieveChunkResponse call() throws Exception {

        Sm.RetrieveChunkResponse chunkResponse = null;

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        FileRunnerMessagePipeline pipeline = new FileRunnerMessagePipeline();
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        for (Sm.StorageNode sn: replicas.getNodeListList()) {

            ChannelFuture cf = bootstrap.connect(sn.getIp(), sn.getPort());
            cf.syncUninterruptibly();

            Sm.RetrieveChunk request = Sm.RetrieveChunk.newBuilder()
                    .setChunkName(chunkName)
                    .setFilename(filename)
                    .build();

            Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                    .setRetrieveChunk(request)
                    .build();

            Channel chan = cf.channel();
            FileRunnerInboundHandler handler = chan.pipeline().get(FileRunnerInboundHandler.class);
            Sm.StorageMessageWrapper response = handler.request(msgWrapper);


            if (response.getRetrieveChunkResponse() != null) {
                chunkResponse = response.getRetrieveChunkResponse();
                if (chunkResponse.getChunkName().equals(chunkName)) {
                    return chunkResponse;
                }
            } else {
                System.out.println("\nSorry but we got a NULL\n");
                continue;
            }

            /* Don't quit until we've disconnected: */
//            System.out.println("Shutting down");
        }
        workerGroup.shutdownGracefully();
        return chunkResponse;
    }
}

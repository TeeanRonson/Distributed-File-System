package edu.usfca.cs.dfs.StorageNode.RelayClient;

import edu.usfca.cs.dfs.Client.ClientInboundHandler;
import edu.usfca.cs.dfs.Sm;
import edu.usfca.cs.dfs.StorageNode.StorageNodeBrain;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Relay {

    private EventLoopGroup workerGroup;
    private RelayMessagePipeline pipeline;
    private Bootstrap bootstrap;

    public Relay() {
        this.workerGroup = new NioEventLoopGroup();
        this.pipeline = new RelayMessagePipeline();
        this.bootstrap = createBootStrap();
    }


    /**
     * Create the Bootstrap Netty Connection
     * @return
     */
    private Bootstrap createBootStrap() {

        return new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);
    }


    /**
     * Collect the data we need to send to the ControllerDriver
     * The heartbeat contains:
     * 1. the free space available
     * 2. total number of requests processed
     * 4. IP
     * 5. port
     * (storage, retrievals, etc.).
     */
    private void collect() {

    }

    public boolean sendData(Sm.StoreChunk chunk, Sm.StorageNodes nodes, String host, int port) {

        System.out.println("SENDING TO: ");
        System.out.println(host);
        System.out.println(port);

        ChannelFuture cf = bootstrap.connect(host, port);
        cf.syncUninterruptibly();

        //Build StorageChunkRequest
        Sm.StoreChunkRequest chunkRequest = Sm.StoreChunkRequest.newBuilder()
                .setChunk(chunk)
                .setNodes(nodes)
                .build();

        //Build MsgWrapper
        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                .setScRequest(chunkRequest)
                .build();

        //Relay the request
        System.out.println("Relaying Request!");
        Channel chan = cf.channel();
        RelayInboundHandler handler = chan.pipeline().get(RelayInboundHandler.class);
        Sm.StorageMessageWrapper response = handler.request(msgWrapper);

        killRunnable();
        return response.getScResponse().getSuccess();

    }

    private void killRunnable() {
        workerGroup.shutdownGracefully();

    }

}
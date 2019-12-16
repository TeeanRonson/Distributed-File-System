package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.usfca.cs.dfs.Sm;
import edu.usfca.cs.dfs.StorageNode.StorageNodeBrain;
import io.netty.channel.*;

@ChannelHandler.Sharable
public class InboundHandler extends SimpleChannelInboundHandler<Sm.StorageMessageWrapper> {

    private Channel channel;
    private Sm.StorageMessageWrapper resp;
    BlockingQueue<Sm.StorageMessageWrapper> resps = new LinkedBlockingQueue<>();
    private StorageNodeBrain nodeBrain;
    public InboundHandler(String filename) {
        this.nodeBrain = StorageNodeBrain.getInstance(filename);
        this.nodeBrain.runHeartBeats();
    }
    

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        channel = ctx.channel();
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Sm.StorageMessageWrapper msgWrapper) {
        System.out.println("Reading response in ChannelRead0");
//        Sm.StoreChunk storeChunkMsg = msg.getStoreChunkMsg();
//        System.out.println("Storing file name: " + storeChunkMsg.getFileName());

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
        System.out.println("Reading response in ChannelRead------------------------------");

        this.nodeBrain.handleIncomingRequest(ctx, (Sm.StorageMessageWrapper)object);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}

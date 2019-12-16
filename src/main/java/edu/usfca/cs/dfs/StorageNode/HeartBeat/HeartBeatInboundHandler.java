package edu.usfca.cs.dfs.StorageNode.HeartBeat;

import edu.usfca.cs.dfs.Sm;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@ChannelHandler.Sharable
public class HeartBeatInboundHandler extends SimpleChannelInboundHandler<Sm.StorageMessageWrapper> {

    private Channel channel;
    private Sm.StorageMessageWrapper resp;
    BlockingQueue<Sm.StorageMessageWrapper> resps = new LinkedBlockingQueue<>();
    public HeartBeatInboundHandler() {

    }


//    /**
//     * Gets the received Request from the BQ
//     * @return
//     */
//    public Sm.StorageMessageWrapper getRequest() {
//
////        System.out.println("Fetching Request");
//
//        boolean interrupted = false;
//        for (;;) {
//            try {
//                Sm.StorageMessageWrapper curr = resps.take();
//                System.out.println("Responses1: " + resps.isEmpty());
//                resp = curr;
//                System.out.println("Responses2: " + resps.isEmpty());
//                break;
//            } catch (InterruptedException ignore) {
//                interrupted = true;
//            }
//        }
//        if (interrupted) {
//            Thread.currentThread().interrupt();
//        }
//        return resp;
//    }


//    /**
//     * Await the server response after a sent request
//     * @return
//     */
//    public Sm.StorageMessageWrapper awaitResponse() {
//
//        boolean interrupted = false;
//        for (;;) {
//            try {
//                System.out.println("Responses: " + resps.isEmpty() + " " + resps.remainingCapacity());
//                Sm.StorageMessageWrapper curr = resps.take();
//                System.out.println("Here: " + curr.getAsnResponse().getFileName());
//                resp = curr;
//                System.out.println("Responses1: " + resps.isEmpty());
//                break;
//            } catch (InterruptedException ignore) {
//                interrupted = true;
//            }
//        }
//        if (interrupted) {
//            Thread.currentThread().interrupt();
//        }
//        return resp;
//    }

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
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("Reading response in ChannelRead");
//        System.out.println("filename = " + ((Sm.StorageMessageWrapper) msg).getStoreChunkMsg().getFileName());
//        this.nodeBrain.handleIncomingRequest();
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

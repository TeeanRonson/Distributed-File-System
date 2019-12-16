package edu.usfca.cs.dfs.Client.Runners;

import edu.usfca.cs.dfs.Sm;
import io.netty.channel.*;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@ChannelHandler.Sharable
public class FileRunnerInboundHandler extends SimpleChannelInboundHandler<Sm.StorageMessageWrapper> {

    private Channel channel;
    private Sm.StorageMessageWrapper resp;
    BlockingQueue<Sm.StorageMessageWrapper> resps = new LinkedBlockingQueue<>();
    public FileRunnerInboundHandler() {

    }


     public Sm.StorageMessageWrapper request(Sm.StorageMessageWrapper msg) {

//        System.out.println("Sending a request");
        ChannelFuture write = channel.writeAndFlush(msg);
        write.addListener((ChannelFuture future) -> {

                if (future.isSuccess()) {
//                    System.out.println("Success");
                } else {
                    System.out.println("Could not deliver Request");
                    ;
                }

        });


        return awaitResponse();
    }

    public Sm.StorageMessageWrapper awaitResponse() {

        boolean interrupted = false;
        for (;;) {
            try {
                Sm.StorageMessageWrapper curr = resps.take();
                resp = curr;
                break;
            } catch (InterruptedException ignore) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return resp;
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
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        System.out.println("Reading response in ChannelRead");
//        System.out.println("filename = " + ((Sm.StorageMessageWrapper) msg).getStoreChunkMsg().getFileName());
        resps.add((Sm.StorageMessageWrapper)msg);
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

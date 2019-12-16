package edu.usfca.cs.dfs.Controller;

import edu.usfca.cs.dfs.Sm;
import io.netty.channel.*;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@ChannelHandler.Sharable
public class ControllerInboundHandler extends SimpleChannelInboundHandler<Sm.StorageMessageWrapper> {

    private Channel channel;
    private Sm.StorageMessageWrapper resp;
    BlockingQueue<Sm.StorageMessageWrapper> resps = new LinkedBlockingQueue<>();
    private Controller serverHandler;
    public ControllerInboundHandler(String configuration) {
        this.serverHandler = Controller.getInstance(configuration);
    }


    public Sm.StorageMessageWrapper request(Sm.StorageMessageWrapper msg) {

        System.out.println("Sending a request");
        ChannelFuture write = channel.writeAndFlush(msg);
        write.addListener((ChannelFuture future) -> {

            if (future.isSuccess()) {
                System.out.println("Success");
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
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("1. Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("3. Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
        System.out.println("Channel Writability Changed");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Sm.StorageMessageWrapper msg) {

//        resps.add(msg);

        this.serverHandler.handleRequest(ctx, msg);
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        System.out.println("Exception Caught");
        cause.printStackTrace();
    }

}

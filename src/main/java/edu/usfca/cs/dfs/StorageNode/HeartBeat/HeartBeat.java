package edu.usfca.cs.dfs.StorageNode.HeartBeat;

import edu.usfca.cs.dfs.CONSTANTS;
import edu.usfca.cs.dfs.Sm;
import edu.usfca.cs.dfs.StorageNode.StorageNodeBrain;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.TimeUnit;

public class HeartBeat implements Runnable {

    private final int DEFAULT_PERIOD = 5; //seconds
//    private EventLoopGroup workerGroup;
//    private HeartBeatMessagePipeline pipeline;
//    private Bootstrap bootstrap;
    private StorageNodeBrain nodeBrain;
    private boolean isRunning;

    public HeartBeat() {
//        this.workerGroup = new NioEventLoopGroup();
//        this.pipeline = new HeartBeatMessagePipeline();
//        this.bootstrap = createBootStrap();
        this.isRunning = false;
        this.nodeBrain = StorageNodeBrain.getInstance();
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

    public void start() {
        this.isRunning = true;
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

    public void sendData(){
        /** Here you should send the data to the server. Use REST/SOAP/multicast messages, whatever you want/need/are forced to **/
    }

    public void killRunnable() {
        this.isRunning = false;
    }

    public void run() {

        try {
            while(isRunning){

                EventLoopGroup workerGroup = new NioEventLoopGroup();
                HeartBeatMessagePipeline pipeline = new HeartBeatMessagePipeline();
                Bootstrap bootstrap = new Bootstrap()
                        .group(workerGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(pipeline);


                System.out.println("Sending a Heartbeat");

                TimeUnit.SECONDS.sleep(CONSTANTS.HEARTBEAT_INTERVAL_SECONDS);

                ChannelFuture cf = bootstrap.connect(this.nodeBrain.getControllerHost(), this.nodeBrain.getControllerPort());
                cf.syncUninterruptibly();

                Sm.HeartBeat heartBeat = Sm.HeartBeat.newBuilder()
                        .setFreeSpace(this.nodeBrain.getAvailableSpace())
                        .setProcessedRequests(this.nodeBrain.getTotalProcessed())
                        .setIp(this.nodeBrain.getSelfHost())
                        .setPort(this.nodeBrain.getSelfPort())
                        .setId(this.nodeBrain.getSelfId())
                        .build();

                Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
                        .setHeartBeat(heartBeat)
                        .build();

                System.out.println("Wrapped Heartbeat: \n" + msgWrapper.getHeartBeat());

                Channel chan = cf.channel();
                ChannelFuture write = chan.write(msgWrapper);
                chan.flush();
                write.syncUninterruptibly();

                /* Don't quit until we've disconnected: */
                System.out.println("Shutting down");
                workerGroup.shutdownGracefully();
            }

        } catch (InterruptedException e) {
            System.out.println("Thread can't send heart beat");
        }
    }
}
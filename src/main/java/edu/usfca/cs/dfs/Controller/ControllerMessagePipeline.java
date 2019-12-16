package edu.usfca.cs.dfs.Controller;

import edu.usfca.cs.dfs.Sm;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

public class ControllerMessagePipeline extends ChannelInitializer<SocketChannel> {

    private ControllerInboundHandler controllerHandler;

    public ControllerMessagePipeline(String configuration) {
        controllerHandler = new ControllerInboundHandler(configuration);
    }


    @Override
    public void initChannel(SocketChannel ch) throws Exception {
//        System.out.println("ControllerDriver Message Pipeline");
        ChannelPipeline pipeline = ch.pipeline();

        /* Inbound: */
        /* For the LengthFieldBasedFrameDecoder, set the maximum frame length
         * (first parameter) based on your maximum chunk size plus some extra
         * space for additional metadata in your proto messages. Assuming a
         * chunk size of 100 MB, we'll use 128 MB here. We use a 4-byte length
         * field to give us 32 bits' worth of frame length, which should be
         * plenty for the future... */
        pipeline.addLast(
                new LengthFieldBasedFrameDecoder(128*1048576, 0, 4, 0, 4));
        pipeline.addLast(
                new ProtobufDecoder(
                    Sm.StorageMessageWrapper.getDefaultInstance()));
        pipeline.addLast(controllerHandler);

        /* Outbound: */
        pipeline.addLast(new LengthFieldPrepender(4));
        pipeline.addLast(new ProtobufEncoder());
    }
}

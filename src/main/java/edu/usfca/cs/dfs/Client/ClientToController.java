package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.Client.Objects.FileInfoObject;
import edu.usfca.cs.dfs.Sm;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.Map;

public class ClientToController {


    public ClientToController() {


    }




//    /**
//     * Sends Messages to controller
//     * @param msgWrapper
//     * @return
//     */
//    public Sm.StorageMessageWrapper createChannelAndSend(Sm.StorageMessageWrapper msgWrapper) {
//
//        ChannelFuture cf = client.getBootstrap().connect(client.getController(), client.getControllerPort());
//        cf.syncUninterruptibly();
//
//        Channel chan = cf.channel();
//        ClientInboundHandler handler = chan.pipeline().get(ClientInboundHandler.class);
//        Sm.StorageMessageWrapper response = handler.request(msgWrapper);
//
//        System.out.println("Shutting down connection");
//        client.workerGroup.shutdownGracefully();
//
//        return response;
//    }


    /**
     * Send an AssignStorageNodesRequest to the ControllerDriver and receive
     * a response with a map of chunk -> StorageNodes
     * @param request
     */
//    public Map<String, Sm.StorageNodes> sendAssignStorageNodesRequest(Sm.AssignStorageNodesRequest request) {
//
////        ChannelFuture cf = bootstrap.connect(this.controller, controllerPort);
////        cf.syncUninterruptibly();
//
//        //Build MsgWrapper
//        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder().setAsnRequest(request).build();
//        //Send the Request
////        Channel chan = cf.channel();
////        ClientInboundHandler handler = chan.pipeline().get(ClientInboundHandler.class);
//
//
//        Sm.StorageMessageWrapper response = createChannelAndSend(msgWrapper);
//
////        System.out.println("Got the Response: " + response.getAsnResponse().getFileName());
//
//        return response.getAsnResponse().getChunksToReplicasMap();
//    }
//
//    /**
//     * Sends a RetrieveFileLocation Request to the ControllerDriver
//     * @param fileInfo
//     * @return
//     */
//    public Sm.RetrieveFileLocationResponse sendRetrieveFileLocationRequest(FileInfoObject fileInfo) {
//
//
//        System.out.println("RetrieveFiles");
//
//
////        ChannelFuture cf = bootstrap.connect(this.controller, controllerPort);
////        cf.syncUninterruptibly();
//
//        Sm.RetrieveFileLocationRequest request = Sm.RetrieveFileLocationRequest.newBuilder()
//                .setFileName(fileInfo.getFilename())
//                .addAllChunks(fileInfo.getFileChunks())
//                .build();
//
//        Sm.StorageMessageWrapper msgWrapper = Sm.StorageMessageWrapper.newBuilder()
//                .setRflRequest(request)
//                .build();
//
//
////        Channel chan = cf.channel();
////        ClientInboundHandler handler = chan.pipeline().get(ClientInboundHandler.class);
////        Sm.StorageMessageWrapper response = handler.request(msgWrapper);
//
//        Sm.StorageMessageWrapper response = createChannelAndSend(msgWrapper);
//
//        System.out.println("Got the Response: " + response.getRflResponse());
//
//        return response.getRflResponse();
//
//    }
}

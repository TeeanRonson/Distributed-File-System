package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class StorageServer {

    ServerMessageRouter nodeListener;

    public StorageServer() {

    }

    public void start(String filename, int nodePort) {
        nodeListener = new ServerMessageRouter(filename);
        nodeListener.listen(nodePort);
        System.out.println("Listening for connections on port: " + nodePort);
    }

}

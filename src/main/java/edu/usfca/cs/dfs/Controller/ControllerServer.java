package edu.usfca.cs.dfs.Controller;

import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class ControllerServer {


    ControllerMessageRouter nodeListener;

    public ControllerServer() {

    }

    public void start(String filename, int nodePort) {
        nodeListener = new ControllerMessageRouter(filename);
        nodeListener.listen(nodePort);
        System.out.println("Listening for connections on port: " + nodePort);
    }
}

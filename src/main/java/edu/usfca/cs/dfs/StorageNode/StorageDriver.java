package edu.usfca.cs.dfs.StorageNode;

import java.io.IOException;

public class StorageDriver {


    /**
     * On startup: provide a storage directory path and the hostname/IP of the controller.
     * Any old files present in the storage directory should be removed.
     * @param args
     */
    public static void main(String[] args) throws IOException {

        //
        // TODO pass in nodePort and configFile
//        int nodePort = 7701;
//        String configuration = "SN.json";

        String configuration = args[0];
        int port = Integer.parseInt(args[1]);
        StorageServer server = new StorageServer();
//        server.start(configuration, nodePort);
        server.start(configuration, port);


    }
}

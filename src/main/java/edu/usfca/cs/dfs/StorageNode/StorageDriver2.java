package edu.usfca.cs.dfs.StorageNode;

import java.io.IOException;

public class StorageDriver2 {


    /**
     * On startup: provide a storage directory path and the hostname/IP of the controller.
     * Any old files present in the storage directory should be removed.
     * @param args
     */
    public static void main(String[] args) throws IOException {


        //TODO pass in nodePort and configFile from CLI
        int nodePort = 7702;
        String filename = "SN2.json";
        StorageServer server = new StorageServer();
        server.start(filename, nodePort);


    }
}

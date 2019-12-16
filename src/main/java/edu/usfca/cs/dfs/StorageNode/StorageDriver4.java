package edu.usfca.cs.dfs.StorageNode;

import java.io.IOException;

public class StorageDriver4 {


    /**
     * On startup: provide a storage directory path and the hostname/IP of the controller.
     * Any old files present in the storage directory should be removed.
     * @param args
     */
    public static void main(String[] args) throws IOException {

        //TODO pass in nodePort and configFile
        int nodePort = 7704;
        String filename = "SN4.json";
        StorageServer server = new StorageServer();
        server.start(filename, nodePort);


    }
}

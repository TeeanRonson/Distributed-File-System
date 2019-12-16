package edu.usfca.cs.dfs.StorageNode;

import java.io.IOException;

public class StorageDriver8 {


    public static void main(String[] args) throws IOException {


        //TODO pass in nodePort and configFile from CLI
        int nodePort = 7708;
        String filename = "SN8.json";
        StorageServer server = new StorageServer();
        server.start(filename, nodePort);

    }
}

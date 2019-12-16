package edu.usfca.cs.dfs.StorageNode;

import java.io.IOException;

public class StorageDriver7 {


    public static void main(String[] args) throws IOException {


        //TODO pass in nodePort and configFile from CLI
        int nodePort = 7707;
        String filename = "SN7.json";
        StorageServer server = new StorageServer();
        server.start(filename, nodePort);

    }
}

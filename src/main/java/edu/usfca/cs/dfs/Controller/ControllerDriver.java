package edu.usfca.cs.dfs.Controller;

import java.io.IOException;


/**
 * The ControllerDriver is responsible for managing resources in the system, somewhat like an HDFS NameNode.
 */
public class ControllerDriver {


    public static void main(String[] args) throws IOException {

        //TODO: Dynamically input configuration file and port number

//        String configuration = "controller.json";

        String configuration = args[0];
        int port = Integer.parseInt(args[1]);
        ControllerServer server = new ControllerServer();
        server.start(configuration, port);

//        server.start(configuration, 7775);

    }
}


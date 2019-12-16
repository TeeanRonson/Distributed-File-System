package edu.usfca.cs.dfs.Client;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class ClientDriver {

    /**
     * Prompt user and provides input options
     */
    private static void userPrompt() {

        System.out.print("\nPlease select from the following commands: \n" +
                "1. Store\n" +
                "2. Retrieve\n" +
                "3. GetAllNodeData\n" +
                "4. Exit\n");
    }

    private static boolean validateInput(String[] input) {

        if (input.length > 1 || input.length == 0 || input[0].equals(" ")) {
            System.out.println("Please try again. ");
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws IOException {

        //TODO: Run program by giving it only the configuration file and and port to listen on

//        String fileName = "FileToRead.txt";
//        String howToReadAPaper = "howToReadAPaper.pdf";
//        String atlantic = "Atlantic.pdf";
//        String resume = "TianRongLiewResume.pdf";
//        String test1 = "Test1.pdf";
//        int chunkSize = 10;
//        int chunkSize1 = 100;
//        int chunkSize2 = 50;
//
//        Client client = Client.getInstance(configuration);
////
//
//        if (client.storeNewFile(howToReadAPaper, 1000)) {
//            System.out.println("YOUR FILE HAS BEEN STORED!!!");
//        }
//
//        try {
//            TimeUnit.SECONDS.sleep(5);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("-----------------------------------------------");
//
//        client.retrieveFile(howToReadAPaper);
//
//        System.out.println("-----------------------------------------------");

//        client.getAllStorageNodeData();

//        String configuration = "client.json";

        String configuration = args[0];
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Scanner scanner = new Scanner(System.in);
        Client client = Client.getInstance(configuration);


        while(true) {
            String inputFile = "";
            int chunkSize = 0;
            userPrompt();
            String[] input = br.readLine().toLowerCase().split("\\s+");

            if (!validateInput(input)) continue;

            switch(input[0]) {
                case "store":
                    System.out.println("Enter the file name:");
                    inputFile = br.readLine();
                    System.out.println("Enter the chunk size:");
                    chunkSize = Integer.parseInt(br.readLine());
                    if (client.storeNewFile(inputFile, chunkSize)) {
                        System.out.println("Your file has been successfully stored.\n\n");
                    } else {
                        System.out.println("Your file could not be stored. Please try again..");
                    }
                    continue;
                case "retrieve":
                    System.out.println("Enter the file name");
                    inputFile = br.readLine();
                    if (client.retrieveFile(inputFile)) {
                        System.out.println("Your file has been retrieved and saved in the local directory.");
                    } else {
                        System.out.println("We were unable to retrieve the file. ");
                    }
                    continue;

                case "getallnodedata":
                    client.getAllStorageNodeData();
                    continue;

                case "exit":
                    System.exit(0);
                    break;

                default:
                    System.out.println("Invalid input.");
            }
        }
    }
}

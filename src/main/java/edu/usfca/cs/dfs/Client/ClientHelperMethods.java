package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.Client.Objects.FileInfoObject;
import edu.usfca.cs.dfs.Sm;

import java.util.ArrayList;

public class ClientHelperMethods {

    /**
     * Helper method to return a FileInfoObject
     * @param storeChunks
     * @param fileName
     * @param fileSize
     * @param fileChunks
     * @return
     */
    public static FileInfoObject generateFileInfoObject(ArrayList<Sm.StoreChunk> storeChunks, String fileName, int fileSize, ArrayList<Sm.ChunkInfo> fileChunks) {

        Sm.AssignStorageNodesRequest asnR = Sm.AssignStorageNodesRequest.newBuilder()
                .setFileName(fileName)
                .setFileSize(fileSize)
                .addAllChunks(fileChunks)
                .build();

        return new FileInfoObject(storeChunks, asnR, fileChunks, fileName);
    }

}

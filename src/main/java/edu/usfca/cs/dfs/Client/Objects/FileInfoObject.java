package edu.usfca.cs.dfs.Client.Objects;

import edu.usfca.cs.dfs.Sm;
import java.util.ArrayList;

public class FileInfoObject {


    private ArrayList<Sm.StoreChunk> storeChunks;
    private Sm.AssignStorageNodesRequest request;
    private ArrayList<Sm.ChunkInfo> fileChunks;
    private String filename;

    public FileInfoObject(ArrayList<Sm.StoreChunk> storeChunks, Sm.AssignStorageNodesRequest request, ArrayList<Sm.ChunkInfo> fileChunks, String filename) {
        this.storeChunks = storeChunks;
        this.request = request;
        this.filename = filename;
        this.fileChunks = fileChunks;
    }

    public ArrayList<Sm.StoreChunk> getStoreChunks() {
        return storeChunks;
    }

    public void setStoreChunks(ArrayList<Sm.StoreChunk> storeChunks) {
        this.storeChunks = storeChunks;
    }

    public Sm.AssignStorageNodesRequest getRequest() {
        return request;
    }

    public void setRequest(Sm.AssignStorageNodesRequest request) {
        this.request = request;
    }


    public ArrayList<Sm.ChunkInfo> getFileChunks() {
        return fileChunks;
    }

    public void setFileChunks(ArrayList<Sm.ChunkInfo> fileChunks) {
        this.fileChunks = fileChunks;
    }

    public String getFilename() {
        return filename;
    }
}

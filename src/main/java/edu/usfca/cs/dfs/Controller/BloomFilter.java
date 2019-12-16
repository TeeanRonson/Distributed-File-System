package edu.usfca.cs.dfs.Controller;
import com.sangupta.murmur.Murmur3;

import java.util.ArrayList;
import java.util.BitSet;

public class BloomFilter {

    BitSet bloom;
    int hashFunctions;
    int size;
    int bloomSize;
    int bitsSet;
    public BloomFilter(int totalBits, int totalHashFunctions) {
        this.bloom = new BitSet(totalBits);
        this.hashFunctions = totalHashFunctions;
        this.size = totalBits;
        this.bloomSize = 0;
        this.bitsSet = 0;
    }


    /**
     * Put items into the Bitset
     * @param data
     */
    public void put(byte[] data) {

        ArrayList<Long> hashes = new ArrayList<Long>();

        long hash1 = hash(data, 0);
        long hash2 = hash(data, hash1);

        for (int i = 0; i < this.hashFunctions; i++) {
            long hash = (hash1 + i * hash2);
            if (hash < 0) {
                hash = ~hash;
            }
            hashes.add(hash);
        }

        for (long hash: hashes) {
            this.bloom.set((int) (hash % this.size));
            this.bloomSize += 1;
        }

    }



    /**
     * Get a value
     * @param data
     * @return
     */
    public boolean get(byte[] data) {

        ArrayList<Long> hashes = new ArrayList<Long>();
        long hash1 = hash(data, 0);
        long hash2 = hash(data, hash1);

        for (int i = 0; i < this.hashFunctions; i++) {
            long hash = (hash1 + i * hash2);
            if (hash < 0) {
                hash = ~hash;
            }
            hashes.add(hash);
        }

        for (long hash: hashes) {
            if (this.bloom.get((int) (hash % this.size))) {
                return true;
            }
        }
        return false;
    }


    /**
     * Return a Hashed value of the given input
     * @param input
     * @return
     */
    private long hash(byte[] input, long seed) {

        long hash = Murmur3.hash_x86_32(input, input.length, seed);
        return hash;
    }

    /**
     * Returns the false positive probability for the filter given its current number of elements
     * @return
     */
    private float falsePositiveProb() {

        System.out.println(this.size);
        System.out.println(this.bloomSize);
        System.out.println(this.hashFunctions);

        return (float) Math.pow((1 - Math.exp(-this.hashFunctions / (this.size / this.bloomSize))), this.hashFunctions);
    }


//    public static void main(String[] args) {
//
//        BloomFilter bf = new BloomFilter(10, 3);
//
//        String one = "Rong";
//        String two = "Liew";
//        String three = "Tian";
//        String four = "Teean";
//
//        bf.put(one.getBytes());
//        bf.put(two.getBytes());
////        bf.put(three.getBytes());
//        bf.put(four.getBytes());
//////        bf.put(new String("word").getBytes());
//        System.out.println(bf.get(one.getBytes()));
//        System.out.println(bf.get(two.getBytes()));
//        System.out.println(bf.get(three.getBytes()));
//        System.out.println(bf.get(four.getBytes()));
//        System.out.println(bf.falsePositiveProb());
//
//
//
//    }


}





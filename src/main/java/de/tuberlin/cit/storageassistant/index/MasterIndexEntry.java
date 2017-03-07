package de.tuberlin.cit.storageassistant.index;

import org.apache.commons.io.Charsets;

public class MasterIndexEntry {
    int firstHash;
    int lastHash;
    long startPosition;
    long endPosition;

    public MasterIndexEntry(int firstHash, int lastHash, long startPosition, long endPosition) {
        this.firstHash = firstHash;
        this.lastHash = lastHash;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    public byte[] getBytes() {
        return this.toString().getBytes(Charsets.UTF_8);
    }

    @Override
    public String toString() {
        return String.format("%d %d %d %d \n", firstHash, lastHash, startPosition, endPosition);
    }
}

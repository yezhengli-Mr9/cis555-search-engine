package edu.upenn.cis455.storage;

import java.io.Serializable;
import java.util.Arrays;

public class PageData implements Serializable {
    private static final long serialVersionUID = 8186211293559204559L;
    byte[] data;
    long lastRetrieved;
    
    public PageData(byte[] data, long lastRetrieved) {
        this.data = data;
        this.lastRetrieved = lastRetrieved;
    }
    
    public boolean samePage(byte[] content) {
        return Arrays.equals(data, content);
    }
    
    public void updateTime(long time) {
        lastRetrieved = time;
    }
}

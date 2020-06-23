package edu.upenn.cis455.storage;

import java.io.Serializable;

public class PageKey implements Serializable {
    private static final long serialVersionUID = -2769303575430073850L;
    String URL;
    
    public PageKey(String URL) {
        this.URL = URL;
    }
}

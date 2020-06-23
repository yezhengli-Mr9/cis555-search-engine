package edu.upenn.cis455.storage;

import java.io.Serializable;

public class UserKey implements Serializable {
    private static final long serialVersionUID = -8219650871756318382L;
    private String username;
    
    public UserKey(String username) {
        this.username = username;
    }
    
    public final String getUsername() {
        return username;
    }

    public String toString() {
        return "[UserKey: username=" + username + ']';
    }
}

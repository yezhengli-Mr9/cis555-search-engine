package edu.upenn.cis455.storage;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class UserData implements Serializable {
    private static final long serialVersionUID = -8617857940625797061L;
    private byte[] password;
    private String firstName;
    private String lastName;
    
    public UserData(String firstName, String lastName, String password) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        this.password = md.digest(password.getBytes(StandardCharsets.UTF_8));
        this.firstName = firstName;
        this.lastName = lastName;
    }
    
    public boolean samePassword(String password) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] pass = md.digest(password.getBytes(StandardCharsets.UTF_8));
        return Arrays.equals(this.password, pass);
    }
    
    public String getFirstName() {
        return firstName;
    }
    
    public String getLastName() {
        return lastName; 
    }
}

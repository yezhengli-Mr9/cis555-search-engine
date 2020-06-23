package edu.upenn.cis455.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class StorageServer
{
	public static void main(String[] args) throws IOException {
		StorageServer s = new StorageServer("/home/cis455/git/Crawler/worker1");
		
		Queue<URL> urlQueue = new ConcurrentLinkedQueue<URL>();
		Set<URL> urlSeen = ConcurrentHashMap.newKeySet();
		urlQueue.add(new URL("http://crawltest.cis.upenn.edu:80"));
		urlSeen.add(urlQueue.peek());
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(urlQueue);
		s.putBytes("QUEUE", bos.toByteArray(), "doc_store");
		
		bos = new ByteArrayOutputStream();
         oos = new ObjectOutputStream(bos);
        oos.writeObject(urlSeen);
		s.putBytes("SEEN", bos.toByteArray(), "doc_store");
	}
	
	private MyDatabase myDB;
	// Constructor for storage server
	public StorageServer(String dir)
	{
		try {
			myDB = new MyDatabase(dir);
		} catch(DatabaseException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	public void addUserInfo(String key, String val, String DBname)
	{
		// Declare the database entry key-value pair that need to be stored in userDB
		DatabaseEntry theVal = new DatabaseEntry();
		DatabaseEntry theKey = new DatabaseEntry();
		// Associate the bindings with the database entries
		myDB.getUserKeyBinding().objectToEntry(key, theKey);
		myDB.getUserValBinding().objectToEntry(val, theVal);
		// Begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			// Insert key-value pair in userDB
			//this.myDB.getUserDB().put(null, theKey, theVal);
			this.myDB.getDBByName(DBname).put(txn, theKey, theVal);
			// Commit the transaction
			txn.commit();
		} catch(Exception e) {
			e.printStackTrace();
			if (txn != null)
			{
				txn.abort();
				txn = null;
			}
		}
	}
	public String getUserInfo(String key, String DBname)
	{
		// Declare the database entry key-value pair that need to be stored in userDB
		DatabaseEntry theVal = new DatabaseEntry();
		DatabaseEntry theKey = new DatabaseEntry();
		boolean notfound = false;
		// Associate the bindings with the database entries
		myDB.getUserKeyBinding().objectToEntry(key, theKey);
		// Begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			// Insert key-value pair in userDB
			if (this.myDB.getDBByName(DBname).get(txn, theKey, theVal, LockMode.DEFAULT)==OperationStatus.NOTFOUND)
				notfound = true;
			// Commit the transaction
			txn.commit();
		} catch(Exception e) {
			e.printStackTrace();
			if (txn != null)
			{
				txn.abort();
				txn = null;
			}
		}
		// Convert database entry to UserVal
		if (notfound) return null;
		String res = (String)myDB.getUserValBinding().entryToObject(theVal);
		return res;
	}
	public void putBytes(String key, byte[] val, String DBname)
	{
		// Declare the database entry key-value pair that need to be stored in userDB
		DatabaseEntry theVal = new DatabaseEntry(val);
		DatabaseEntry theKey = new DatabaseEntry();
		// Associate the bindings with the database entries
		myDB.getUserKeyBinding().objectToEntry(key, theKey);
		// Begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			// Insert key-value pair in userDB
			//this.myDB.getUserDB().put(null, theKey, theVal);
			this.myDB.getDBByName(DBname).put(txn, theKey, theVal);
			// Commit the transaction
			txn.commit();
		} catch(Exception e) {
			e.printStackTrace();
			if (txn != null)
			{
				txn.abort();
				txn = null;
			}
		}
	}
	public byte[] getBytes(String key, String DBname)
	{
		// Declare the database entry key-value pair that need to be stored in userDB
		DatabaseEntry theVal = new DatabaseEntry();
		DatabaseEntry theKey = new DatabaseEntry();
		boolean notfound = false;
		// Associate the bindings with the database entries
		myDB.getUserKeyBinding().objectToEntry(key, theKey);
		// Begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			// Insert key-value pair in userDB
			if (this.myDB.getDBByName(DBname).get(txn, theKey, theVal, LockMode.DEFAULT)==OperationStatus.NOTFOUND)
				notfound = true;
			// Commit the transaction
			txn.commit();
		} catch(Exception e) {
			e.printStackTrace();
			if (txn != null)
			{
				txn.abort();
				txn = null;
			}
		}
		// Convert database entry to UserVal
		if (notfound) return null;
		return theVal.getData();
	}
//	public String showRecords(String name) {
//		String ret = "";
//		DatabaseEntry theVal = new DatabaseEntry();
//		DatabaseEntry theKey = new DatabaseEntry();
//		Cursor cursor = null;
//		Transaction txn = myDB.getEnv().beginTransaction(null, null);
//		try {
//			// Insert key-value pair in userDB
//			//this.myDB.getUserDB().put(null, theKey, theVal);
//			cursor = this.myDB.getDBByName(name).openCursor(null, null);
//			while (cursor.getNext(theKey, theVal, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
//				String key = (String)myDB.getUserKeyBinding().entryToObject(theKey);
//				if (!key.endsWith("type") && !key.endsWith("modified") && !key.endsWith("time"))
//					ret = ret + key + "\r\n";
//			}
//			// Commit the transaction
//			txn.commit();
//		} catch(Exception e) {
//			e.printStackTrace();
//			if (txn != null)
//			{
//				txn.abort();
//				txn = null;
//			}
//		} finally {
//			cursor.close();
//		}
//		return ret;
//	}
	public HashMap<String, String> showStringRecords(String name) {
		HashMap<String, String> ret = new HashMap<String, String>();
		DatabaseEntry theVal = new DatabaseEntry();
		DatabaseEntry theKey = new DatabaseEntry();
		Cursor cursor = null;
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			// Insert key-value pair in userDB
			//this.myDB.getUserDB().put(null, theKey, theVal);
			cursor = this.myDB.getDBByName(name).openCursor(null, null);
			while (cursor.getNext(theKey, theVal, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				String key = (String)myDB.getUserKeyBinding().entryToObject(theKey);
				String val = (String)myDB.getUserValBinding().entryToObject(theVal);
				ret.put(key, val);
			}
			// Commit the transaction
			txn.commit();
		} catch(Exception e) {
			e.printStackTrace();
			if (txn != null)
			{
				txn.abort();
				txn = null;
			}
		} finally {
			cursor.close();
		}
		return ret;
	}
	
	public HashMap<String, byte[]> showBytesRecords(String name) {
		HashMap<String, byte[]> ret = new HashMap<String, byte[]>();
		DatabaseEntry theVal = new DatabaseEntry();
		DatabaseEntry theKey = new DatabaseEntry();
		Cursor cursor = null;
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			// Insert key-value pair in userDB
			//this.myDB.getUserDB().put(null, theKey, theVal);
			cursor = this.myDB.getDBByName(name).openCursor(null, null);
			while (cursor.getNext(theKey, theVal, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				String key = (String)myDB.getUserKeyBinding().entryToObject(theKey);
				ret.put(key, theVal.getData());
			}
			// Commit the transaction
			txn.commit();
		} catch(Exception e) {
			e.printStackTrace();
			if (txn != null)
			{
				txn.abort();
				txn = null;
			}
		} finally {
			cursor.close();
		}
		return ret;
	}
	public void deleteUserInfo(String key, String DBname) {
		// Declare the database entry key-value pair that need to be stored in userDB
		DatabaseEntry theKey = new DatabaseEntry();
		// Associate the bindings with the database entries
		myDB.getUserKeyBinding().objectToEntry(key, theKey);
		// Begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			// Insert key-value pair in userDB
			this.myDB.getDBByName(DBname).delete(txn, theKey);
			// Commit the transaction
			txn.commit();
		} catch(Exception e) {
			e.printStackTrace();
			if (txn != null)
			{
				txn.abort();
				txn = null;
			}
		}
	}
	public String getSHA(String input) {
		try {
			return new String(MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8)));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}	
	}
}

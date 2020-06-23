package edu.upenn.cis455.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.security.NoSuchAlgorithmException;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
//import com.sleepycat.persist.EntityStore;

/** (MS1, MS2) A wrapper class which should include:
  * - Set up of Berkeley DB
  * - Saving and retrieving objects including crawled docs and user information
  */
public class DBWrapper {
	private static String envDirectory = null;
	private static Environment myEnv;
	private StoredClassCatalog javaCatalog;
	private static final String CLASS_CATALOG = "java_class_catalog";
	private static final String USER_STORE = "user_store";
	private static final String PAGE_STORE = "page_store";
	private Database userDb;
	private Database pageDb; 
	private StoredSortedMap<String, UserData> userMap;
    private StoredSortedMap<String, PageData> pageMap;
	
    public DBWrapper(String envDirectory) throws DatabaseException, FileNotFoundException {
        DBWrapper.envDirectory = envDirectory;
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        
        //envConfig.setInitializeCache(true);
        //envConfig.setInitializeLocking(true);

        myEnv = new Environment(new File(envDirectory), envConfig);
        
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        //dbConfig.setType(DatabaseType.BTREE);
        
        Database catalogDb = myEnv.openDatabase(null, CLASS_CATALOG, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);
        
        userDb = myEnv.openDatabase(null, USER_STORE, dbConfig);
        pageDb = myEnv.openDatabase(null, PAGE_STORE, dbConfig);
        
        ClassCatalog catalog = javaCatalog;
        EntryBinding<String> userKeyBinding = new SerialBinding<String>(catalog, String.class);
        EntryBinding<UserData> userDataBinding = new SerialBinding<UserData>(catalog, UserData.class);
        EntryBinding<String> pageKeyBinding = new SerialBinding<String>(catalog, String.class);
        EntryBinding<PageData> pageDataBinding = new SerialBinding<PageData>(catalog, PageData.class);

        userMap = new StoredSortedMap<String, UserData>(userDb, userKeyBinding, userDataBinding, true);
        pageMap = new StoredSortedMap<String, PageData>(pageDb, pageKeyBinding, pageDataBinding, true);
    }
    
    public void addUser(String user, String firstName, String lastName, String password) {
        if(!userMap.containsKey(user)) {
            try {
                userMap.put(user, new UserData(firstName, lastName, password));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }
    
    public boolean containsUser(String user) {
        return userMap.containsKey(user);
    }
    
    public boolean samePassword(String user, String password) {
        if(userMap.containsKey(user)) {
            try {
                return userMap.get(user).samePassword(password);
            } catch (NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return false;
    }
    
    public String[] getName(String username) {
        if(userMap.containsKey(username)) {
            UserData user = userMap.get(username);
            String[] name = {user.getFirstName(), user.getLastName()};
            return name;
        }
        return null;
    }
    
    public void addPage(String URL, byte[] data, long lastRetrieved) {
        if(!pageMap.containsKey(URL)) {
            pageMap.put(URL, new PageData(data, lastRetrieved));
        }
    }
    
    public Long getTime(String URL) {
        if(pageMap.containsKey(URL)) {
            return pageMap.get(URL).lastRetrieved;
        }
        return null;
    }
    
    public void updateTime(String URL, long lastRetrieved) {
        if(pageMap.containsKey(URL)) {
            PageData page = pageMap.get(URL);
            page.updateTime(lastRetrieved);
        }
    }
    
    public void changePageURL(String oldURL, String newURL) {
        if(pageMap.containsKey(oldURL)) {
            PageData page = pageMap.get(oldURL);
            pageMap.remove(oldURL);
            pageMap.put(newURL, page);
        }
    }
    
    public void removePage(String URL) {
        
    }
    
    public void close() throws DatabaseException {
        userDb.close();
        pageDb.close();
        javaCatalog.close();
        myEnv.close();
    }
}

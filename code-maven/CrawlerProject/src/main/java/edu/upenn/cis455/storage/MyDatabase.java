package edu.upenn.cis455.storage;
import java.io.File;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.*;;

public class MyDatabase
{
	private Environment env = null;
	/*
	 * USER_STORE : Name used to identify userDB
	 * DOC_STORE : Name used to identify docDB
	 * CLASS_CATALOG : Name used to identify the class catalog
	 */
	private static final String CHAN_STORE = "chan_store";
	private static final String DOC_STORE = "doc_store";
	private static final String USER_STORE = "user_store";
	private static final String CLASS_CATALOG = "class_catalog";
	private StoredClassCatalog catalog = null;
	private Database docDB = null;
	private Database userDB = null;
	private Database chanDB = null;
	private Database catalogDB = null;
	// Instantiate the bindings for the key and value classes
	EntryBinding userKeyBinding;
	EntryBinding userValBinding;
	public MyDatabase(String dir)
	{
		try
		{
			// Create EnvironmentConfig object
			EnvironmentConfig envConfig = new EnvironmentConfig();
			// Environment should be capable of performing transactions
			envConfig.setTransactional(true);
			// Create a database environment if it doesn’t already exist
			envConfig.setAllowCreate(true);
			// Instantiate environment
			this.env = new Environment(new File(dir), envConfig);
			// Create DatabaseConfig object
			DatabaseConfig dbConfig = new DatabaseConfig();
			// Encloses the database open within a transaction.
			dbConfig.setTransactional(true);
			// Create the database if it does not already exist
			dbConfig.setAllowCreate(true);
			// Instantiate a catalog database to keep track of the database’s metadata
			Database catalogDB = env.openDatabase(null, CLASS_CATALOG,
					dbConfig);
			this.catalog = new StoredClassCatalog(catalogDB);
			// Instantiate user database
			this.userDB = env.openDatabase(null,USER_STORE, dbConfig);
			this.docDB = env.openDatabase(null, DOC_STORE, dbConfig);
			this.chanDB = env.openDatabase(null, CHAN_STORE, dbConfig);
			// Instantiate the bindings for the key and value classes
			this.userKeyBinding = new SerialBinding(this.catalog, String.class);
			this.userValBinding = new SerialBinding(this.catalog, String.class);
		}
		catch(DatabaseException e)
		{
			throw e;
		}
	}
	// Return Environment instance
	public Environment getEnv()
	{
		return this.env;
	}
	// Return userDB key binding
	public EntryBinding getUserKeyBinding()
	{
		return this.userKeyBinding;
	}
	// Return userDB val binding
	public EntryBinding getUserValBinding()
	{
		return this.userValBinding;
	}
	public Database getDBByName(String name) {
		if (name.equals(DOC_STORE)) return this.docDB;
		else if (name.equals(CHAN_STORE)) return this.chanDB;
		else return this.userDB;
	}
}
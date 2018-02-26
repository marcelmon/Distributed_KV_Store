package app_kvServer;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.util.Arrays;

import java.util.Objects;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FilePerKeyKVDB implements IKVDB {
	/**
     * Iterator implementation to produce key-value Entries when loading data.
     * Called by FilePerKeyKVDB.iterator()
     */
	public class FilePerKeyIterator implements Iterator<Map.Entry<String, String>> {

		private String dataDir;
		private File[] keyValueFileList;
		private int currentIndex = 0;

		public FilePerKeyIterator (String dataDir) {
			File f = new File(dataDir);
			if (!f.exists() || !f.isDirectory()) {
				// throw new InvalidPathException("KVDB data directory path \"" + dataDir + "\" invalid.");
			}
			this.dataDir = dataDir;
			keyValueFileList = f.listFiles();
            logger.debug("FilePerKeyIterator()");
		}


		@Override
        public boolean hasNext() {
            boolean result = currentIndex < keyValueFileList.length && keyValueFileList[currentIndex] != null;
            logger.debug("hasNext() -> result : " + result);
            return result;
        }



        @Override
        public Map.Entry<String, String> next() {
        	File keyValueFile = keyValueFileList[currentIndex];
        	if(keyValueFile.isFile()){
        		try{
        			String allValue = new String(Files.readAllBytes(Paths.get(keyValueFile.getAbsolutePath())));
        			this.currentIndex++;
                    logger.debug("FilePerKeyIterator next(), -> key : " + keyValueFile.getName() + ", value : " + allValue);
		        	// keyValueFile.getName() == keyName,  Arrays.toString(valueBytes) == value
		        	return new SimpleEntry<String, String>(keyValueFile.getName(),  allValue);

        		}
        		catch(IOException e){
                    logger.error("FilePerKeyIterator next() IOException : " + e + " , " + e.getMessage());
        			return null;
        		}
        	}
            logger.debug("FilePerKeyIterator next() file is directory");
        	return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

	}




	protected String dataDir;

    protected static Logger logger = Logger.getRootLogger();

	public FilePerKeyKVDB(String dataDir) {
		
		File f = new File(dataDir);

        if(!f.exists()){
            logger.debug("FilePerKeyKVDB() creating dataDir : " + dataDir);
            f.mkdir();
        }
		if (!f.exists() || !f.isDirectory()) {
			// throw new InvalidPathException("KVDB data directory path \"" + dataDir + "\" invalid.");
            logger.error("FilePerKeyKVDB() error creating dataDir : " + dataDir);
            return;
		}

		this.dataDir = dataDir + "/"; // make sure has the trailing slash
	}






	@Override
    public boolean inStorage(String key) {
    	File f = new File(this.dataDir + key);
    	if(f.exists()){
    		if(f.isFile()){
    			return true;
    		}
            logger.error("FilePerKeyKVDB() key : " + key + " -> file exists but not isFile()");
    		// throw new InvalidPathException("The key data file " + this.dataDir + key + " is a directory.");
    	}
    	return false;
    }

    @Override
    public String get(String key) throws KeyDoesntExistException { // throws ???IOException?
    	
        File f = new File(this.dataDir + key);
    	if(f.exists()){
    		if(f.isFile()){
                try{
                    String allValue = new String(Files.readAllBytes(Paths.get(this.dataDir + key)));
                    return allValue;
                }
                catch(IOException e){
                    logger.debug("get(), key : " + key + ", exception : " + e + ", message : " + e.getMessage());
                }
    		}
            logger.error("get() : " + key + " -> file exists but not isFile()");
    		// throw new InvalidPathException("The key data file " + this.dataDir + key + " is a directory.");
    	}
    	throw new KeyDoesntExistException("The key " + key + " does not exist on disk.");
    }

    @Override
    /*
	 * Note that for pure safety is might be better to write to file then move into place (rather than overwriting from the start)
     * For now it oerwrites from start for simplicity.
     * 
     * If file exists first read the file to make sure content is changing.
     */
    public boolean put(String key, String value) throws Exception {
        System.out.println("DB Put called:" + key + "," + value);
    	File f = new File(this.dataDir + key);
    	if(f.exists()){
    		if(f.isDirectory()){
    			throw new Exception("The key data file " + this.dataDir + key + " is a directory.");
    		}
            try{
                byte[] valueBytes = Files.readAllBytes(Paths.get(this.dataDir + key));
                String existingValue = Arrays.toString(valueBytes);
                if(Objects.equals(existingValue, value)){
                    // throw new Exception("KEY VALUE DID NOT CHANGE");
                    logger.debug("put() key : " + key + ", value : " + value + " -> did not change");
                }
                else{
                    Writer fileWriter = new FileWriter(this.dataDir + key);
                    fileWriter.write(value);
                    fileWriter.close();
                    return false;
                }
                    
            }
            catch(IOException e){
                throw e;
            }
    	}
    	Writer fileWriter = new FileWriter(this.dataDir + key);
		fileWriter.write(value);
		fileWriter.close();
        logger.debug("put() key : " + key + ", value : " + value + " -> changed");
		return true; // was new key
    }
    
    /**
     * Delete the key-value pair from storage.
     */
    public void delete(String key) throws KeyDoesntExistException {
    	File f = new File(this.dataDir + key);
    	if(!f.exists()){
    		throw new KeyDoesntExistException("The key " + key + " does not exist on disk.");
    	}
    	if(f.isDirectory()){
            logger.error("delete() key : " + key + " -> is directory");
			// throw new InvalidPathException("The key data file " + this.dataDir + key + " is a directory.");
		}
		if(f.delete()){
        	return;
        }
        logger.error("delete() key : " + key + " -> f.delete() did not return true");
        // throw new Exception("Error deleting key file " + this.dataDir + key);
    }



   	@Override
    public void clearStorage() {
    	File f = new File(this.dataDir);
    	if (!f.exists() || !f.isDirectory()) {
			// throw new InvalidPathException("KVDB data directory path \"" + dataDir + "\" invalid.");
            logger.error("clearStorage() -> dataDir " + this.dataDir + " invalid");
		}
    	for (File keyFile : f.listFiles()) {
    		if(keyFile.isFile()){
    			keyFile.delete();
    		}
    	}
    	return;
    }

    /**
     * Returns an Iterator of key value pairs that will be used to load data into the cache.
    */
    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
    	File f = new File(this.dataDir);
    	if (!f.exists() || !f.isDirectory()) {
			logger.error("clearStorage() -> dataDir " + this.dataDir + " invalid");
		}

		return new FilePerKeyIterator(dataDir);
    }

	@Override
	public void loadData(Iterator<Entry<String, String>> iterator) throws Exception {
		while (iterator.hasNext()) {
			Entry<String, String> e = iterator.next();
			put(e.getKey(), e.getValue());
		}
	}


    /**
     * Returns an Iterator of keys that will be used to load data into the cache.
    */
    @Override
    public Iterator<String> keyIterator() {
        return new FilePerKeyKVDBKeyIterator();
    }


    public class FilePerKeyKVDBKeyIterator implements Iterator<String> {

        private File[] allFiles;
        private int currentIndex;

        public FilePerKeyKVDBKeyIterator() {
            File f = new File(dataDir);
            if (!f.exists() || !f.isDirectory()) {
                // throw new InvalidPathException("KVDB data directory path \"" + dataDir + "\" invalid.");
            }
            allFiles = f.listFiles();
            currentIndex = 0;
        }

        public boolean hasNext() {
            while(currentIndex < allFiles.length){
                if(allFiles[currentIndex].isDirectory()){
                    currentIndex++;
                    continue;
                }
                return true;
            }   
            return false;
        }

        public String next() throws NoSuchElementException {
            if(hasNext() != true){
                throw new NoSuchElementException("No more files in db");
            }
            String retKey = allFiles[currentIndex].getName();
            currentIndex++;
            return retKey;
        }
    }
}







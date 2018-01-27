package app_kvServer;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.HashMap;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.util.Arrays;

import java.util.Objects;
import java.nio.charset.StandardCharsets;
public class FilePerKeyKVDB implements IKVDB {




	/**
     * Iterator implementation to produce key-value Entries when loading data.
     * Called by FilePerKeyKVDB.iterator()
     */
	public class FilePerKeyIterator implements Iterator<SimpleEntry<String, String>> {

		private String dataDir;
		private File[] keyValueFileList;
		private int currentIndex = 0;

		public FilePerKeyIterator(String dataDir) {
			File f = new File(dataDir);
			if (!f.exists() || !f.isDirectory()) {
				// throw new InvalidPathException("KVDB data directory path \"" + dataDir + "\" invalid.");
			}
			this.dataDir = dataDir;
			keyValueFileList = f.listFiles();
		}


		@Override
        public boolean hasNext() {
            return currentIndex < keyValueFileList.length && keyValueFileList[currentIndex] != null;
        }



        @Override
        public SimpleEntry<String, String> next() {
        	File keyValueFile = keyValueFileList[currentIndex];
        	if(keyValueFile.isFile()){
        		try{
        			String allValue = new String(Files.readAllBytes(Paths.get(keyValueFile.getAbsolutePath())));
        			this.currentIndex++;
		        	// keyValueFile.getName() == keyName,  Arrays.toString(valueBytes) == value
		        	return new SimpleEntry<String, String>(keyValueFile.getName(),  allValue);

        		}
        		catch(IOException e){
        			return null;
        		}
        	}
        	return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

	}




	protected String dataDir;



	public FilePerKeyKVDB(String dataDir) {
		
		File f = new File(dataDir);

        if(!f.exists()){
            f.mkdir();
        }
		if (!f.exists() || !f.isDirectory()) {
			// throw new InvalidPathException("KVDB data directory path \"" + dataDir + "\" invalid.");
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
                    // byte[] valueBytes = Files.readAllBytes(Paths.get(this.dataDir + key));
                    // return Arrays.toString(valueBytes);
                }
                catch(IOException e){

                }
    		}
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
    	File f = new File(this.dataDir + key);
    	if(f.exists()){
    		if(f.isDirectory()){
    			// throw new InvalidPathException("The key data file " + this.dataDir + key + " is a directory.");
    		}
            try{
                byte[] valueBytes = Files.readAllBytes(Paths.get(this.dataDir + key));
                String existingValue = Arrays.toString(valueBytes);
                if(Objects.equals(existingValue, value)){
                    // throw new Exception("KEY VALUE DID NOT CHANGE");
                }
                else{
                    Writer fileWriter = new FileWriter(this.dataDir + key);
                    fileWriter.write(value);
                    fileWriter.close();
                    return false;
                }
                    
            }
            catch(IOException e){

            }
    		
    		Writer fileWriter = new FileWriter(this.dataDir + key);
    		fileWriter.write(value);
    		fileWriter.close();
    		return false; // was updated
    	}
    	Writer fileWriter = new FileWriter(this.dataDir + key);
		fileWriter.write(value);
		fileWriter.close();
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
			// throw new InvalidPathException("The key data file " + this.dataDir + key + " is a directory.");
		}
		if(f.delete()){
        	return;
        }
        // throw new Exception("Error deleting key file " + this.dataDir + key);
    }



   	@Override
    public void clearStorage() {
    	File f = new File(this.dataDir);
    	if (!f.exists() || !f.isDirectory()) {
			// throw new InvalidPathException("KVDB data directory path \"" + dataDir + "\" invalid.");
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
    public Iterator<SimpleEntry<String, String>> iterator() {
    	File f = new File(this.dataDir);
    	if (!f.exists() || !f.isDirectory()) {
			// throw new InvalidPathException("KVDB data directory path \"" + dataDir + "\" invalid.");
		}

		return new FilePerKeyIterator(dataDir);
    }
}







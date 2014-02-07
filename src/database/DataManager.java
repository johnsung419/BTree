package database;

import java.io.*;
import java.util.*;

public abstract class DataManager
{
	private static Map<String, DataFile> _dataFiles = new HashMap<String,DataFile>();

	// All methods are static, therefore constructor is private
	private DataManager()
	{
	}

	public static DataFile createFile(String fileName, Map<String,Integer> descriptor)
	{
		if (_dataFiles.containsKey(fileName) ||
			fileExistsOnDisk(fileName))
		{
			throw new IllegalArgumentException
			   ("DataManager::createFile(): " + fileName + " already exists");
		}
		else
		{
			DataFile newFile = new DataFile(fileName, descriptor);
			_dataFiles.put(fileName, newFile);
			return newFile;
		}
	}
	
	public static void exit()
	{
		Set<Map.Entry<String,DataFile>> s = _dataFiles.entrySet();

        Iterator<Map.Entry<String,DataFile>> it = s.iterator();
        
        while(it.hasNext())
        {
            Map.Entry<String,DataFile> m = it.next();

            m.getValue().dumpFile();
        }
        
        _dataFiles.clear();
	}
	
	private static boolean fileExistsOnDisk(String fileName)
	{
		return new File(fileName).exists();
	}

	public static String print(Map<String,String> record)
	{
		String returnString = "";
		
		Set<Map.Entry<String,String>> s = record.entrySet();

        Iterator<Map.Entry<String,String>> it = s.iterator();
        
        while(it.hasNext())
        {
            Map.Entry<String,String> m = it.next();
            
            returnString += m.getKey() + ": " + m.getValue() + "\n";
        }
        
        System.out.print(returnString);
        
        return returnString;
	}
	
	public static DataFile restoreFile(String fileName)
	{
		if (_dataFiles.containsKey(fileName))
		{
			throw new IllegalArgumentException
			   ("DataManager::restoreFile(): " + fileName + " already exists in memory");
		}
		else if (!fileExistsOnDisk(fileName))
		{
			throw new IllegalArgumentException
			   ("DataManager::restoreFile(): " + fileName + " not found");
		}
		else
		{
			DataFile dataFile = null;
			try
			{
				FileInputStream fis = new FileInputStream(fileName);
				ObjectInputStream in = new ObjectInputStream(fis);
				dataFile = (DataFile)in.readObject();//Read the serializable objects
				in.close();
			}
			catch (IOException ex)
			{
				ex.printStackTrace();
				dataFile = null;
			}
			catch (ClassNotFoundException ex)
			{
				ex.printStackTrace();
				dataFile = null;
			}
			
			if (dataFile != null)
			{
				_dataFiles.put(fileName, dataFile);
			}
			
			return dataFile;
		}
	}
}

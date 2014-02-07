package database;

import java.io.*;

import java.util.*;

public class DataFile implements Serializable
{
	private static final long serialVersionUID = -4126263454830270822L;
	
	private Map<String,Integer> _fields;
	private String _fileName;
	private Map<Integer, DataRecord> _dataRecords;
	private int _nextRecordId;
	
	private List<String> _indexNames;
	private Map<String,Index> _indexes;
	
	private int _modCount;
	
	public DataFile(String fileName, Map<String,Integer> descriptor)
	{
		_fileName = fileName;
		_fields = descriptor;
		_dataRecords = new TreeMap<Integer, DataRecord>();
		_nextRecordId = 0;
		
		_indexNames = new LinkedList<String>();
		_indexes = new HashMap<String,Index>();
		
		_modCount = 0;
	}
	
	public Index createIndex(String indexName, String column)
	{
		if (_indexNames.contains(indexName))
		{
			throw new IllegalArgumentException
			   ("DataFile::createIndex(): " + indexName + " already exists");
		}
		else if (_indexes.containsKey(column))
		{
			throw new IllegalArgumentException
			   ("DataFile::createIndex(): index over " + column + " already exists");
		}
		else
		{
			_indexNames.add(indexName);
			Index newIndex = new Index(this, indexName, column);
			_indexes.put(column, newIndex);
			
			Set<Map.Entry<Integer,DataRecord>> s = _dataRecords.entrySet();
	        Iterator<Map.Entry<Integer,DataRecord>> recordIter = s.iterator();
	        
	        while (recordIter.hasNext())
	        {
	        	Map.Entry<Integer,DataRecord> m = recordIter.next();	        	
	        	DataRecord record = m.getValue();
	        	
	        	if (!record.isDeletePending())
	        	{
	        		Map<String,String> values = record.getValues();

	        		if (values.containsKey(column))
	        		{
	        			newIndex.insertRecord(values.get(column), record);
	        		}
	        	}
	        }
			
			return newIndex;
		}
	}
	
	public Map<String,String> getRecord(int recordId)
	{
		Map<String,String> returnRecord = null;
		
		DataRecord record = _dataRecords.get(new Integer(recordId));
		
		if (record != null)
		{
			if (!record.isDeletePending())
			{
				returnRecord = record.getValues();
			}
		}
		
		return returnRecord;
	}
	
	public int insertRecord(Map<String,String> record)
	{
		// First, validate record:
		
		Iterator<Map.Entry<String,String>> valuesIter = record.entrySet().iterator();
		
		while(valuesIter.hasNext())
		{
			Map.Entry<String,String> m = valuesIter.next();
			
			if (!_fields.containsKey((m.getKey())))
			{
				throw new IllegalArgumentException("DataFile::insertRecord(): " + m.getKey() + " is invalid");
			}
			else if (m.getValue().length() > _fields.get(m.getKey()))
			{
				throw new IllegalArgumentException("DataFile::insertRecord(): " + m.getKey() + " has value whose length exceeds allowable maximum");
			}
		}
		
		DataRecord newRecord = new DataRecord(new Integer(_nextRecordId), record);
		
		_dataRecords.put(new Integer(_nextRecordId), newRecord);
		
		updateIndexes(_nextRecordId, newRecord);

		_nextRecordId++;
		
		_modCount++;
		
		return _nextRecordId - 1;
	}
	
	private void updateIndexes(int recordId, DataRecord record)
	{
		Map<String,String> recordValues = record.getValues();
		
		Iterator<Map.Entry<String,Index>> indexIter = _indexes.entrySet().iterator();

		while(indexIter.hasNext())
		{
			Map.Entry<String,Index> m = indexIter.next();
			
			String indexColumn = m.getKey();
			
			if (recordValues.containsKey(indexColumn))
			{
				m.getValue().insertRecord(recordValues.get(indexColumn), record);
			}
		}
	}
	
	public void dumpFile()
	{
		FileOutputStream fos = null;
        ObjectOutputStream out = null;
       
        try
        {
            fos = new FileOutputStream(_fileName);
            out = new ObjectOutputStream(fos);
            out.writeObject(this);
           
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
        }
	}
	
	public String viewFile()
	{
		String returnString = "";
		
		Set<Map.Entry<Integer,DataRecord>> s = _dataRecords.entrySet();

        Iterator<Map.Entry<Integer,DataRecord>> recordIter = s.iterator();
		
		while (recordIter.hasNext())
		{
			Map.Entry<Integer,DataRecord> m1 = recordIter.next();
			
			DataRecord record = m1.getValue();
			
			if (!record.isDeletePending())
			{
				returnString += String.format("%d:\n", m1.getKey().intValue());//Key of the DataRecord
				
				Map<String,String> values = record.getValues();  //Value of the DataRecord

				Iterator<Map.Entry<String,Integer>> fieldIter = _fields.entrySet().iterator(); //Descriptor

				while (fieldIter.hasNext())
				{
					Map.Entry<String,Integer> m2 = fieldIter.next();

					if (values.containsKey(m2.getKey()))
					{
						returnString += "\t" + m2.getKey() + ": " + values.get(m2.getKey()) + "\n";//Retrieve the key and the value of the DataRecord
					}
				}
			}
		}
		
		System.out.print(returnString);
		
		return returnString;
	}
	
	public void dropFile()
	{
		// Delete all index files
		Iterator<String> it = _indexNames.iterator();
        
        while(it.hasNext())
        {
            String indexName = it.next();
            
            File f = new File(indexName);
            
            if (f.exists())
            {
            	try
            	{
            		boolean success = f.delete();
            		
            		if (!success)
            		{
            			System.out.println("Failed to delete index file: " + indexName);
            		}
            	}
            	catch (Exception e)
            	{
            		System.out.println("Error occurred while trying to delete index file: " + indexName);
            	}
            }
        }
        
        // Delete data file if it exists
        
        File f = new File(_fileName);
        
        if (f.exists())
        {
        	try
        	{
        		boolean success = f.delete();
        		
        		if (!success)
        		{
        			System.out.println("Failed to delete data file: " + _fileName);
        		}
        	}
        	catch (Exception e)
        	{
        		System.out.println("Error occurred while trying to delete data file: " + _fileName);
        	}
        }
        
        clear();
	}
	
	private void clear()
	{
		_dataRecords.clear();
		_nextRecordId = 0;
		
		_indexNames.clear();
		_indexes.clear();
		
		_modCount = 0;
	}
	
	public int getModCount()
	{
		return _modCount;
	}
	
	public void incrementModCount()
	{
		_modCount++;
	}
	
	public Index restoreIndex(String indexName)
	{
		if (_indexNames.contains(indexName))
		{
			throw new IllegalArgumentException
			   ("DataFile::restoreIndex(): " + indexName + " already exists in memory");
		}
		else
		{
			File f = new File(indexName);
			
			if (!f.exists())
			{
				throw new IllegalArgumentException
				   ("DataFile::restoreIndex(): " + indexName + " not found");
			}
			else
			{
				Index index = null;
				
				try
				{
					FileInputStream fis = new FileInputStream(indexName);
					ObjectInputStream in = new ObjectInputStream(fis);
					index = (Index)in.readObject();
					in.close();
				}
				catch (IOException ex)
				{
					ex.printStackTrace();
					index = null;
				}
				catch (ClassNotFoundException ex)
				{
					ex.printStackTrace();
					index = null;
				}

				if (index != null)
				{
					_indexNames.add(indexName);
					_indexes.put(new String(index.getColumnName()), index);
				}
				
				return index;
			}
		}
	}
	
	public void dropIndex(String indexName)
	{
		if (_indexNames.contains(indexName))
		{
			_indexNames.remove(indexName);
			
			Iterator<Map.Entry<String,Index>> indexIter = _indexes.entrySet().iterator();

			String key = null;
			
			while(indexIter.hasNext())
			{
				Map.Entry<String,Index> m = indexIter.next();
				
				if (indexName.compareTo(m.getValue().getIndexName()) == 0)
				{
					key = m.getKey();
					break;
				}
			}
			
			if (key != null)
			{
				_indexes.remove(key);
			}
		}
		
		File f = new File(indexName);
		if (f.exists())
		{
			try
        	{
        		boolean success = f.delete();
        		
        		if (!success)
        		{
        			System.out.println("Failed to delete index file: " + indexName);
        		}
        	}
        	catch (Exception e)
        	{
        		System.out.println("Error occurred while trying to delete index file: " + indexName);
        	}
		}
	}
	
	public void rebuildFile()
	{
		// Here, we simply iterate through all data records and remove those
		// that are marked as deleted:
		Set<Map.Entry<Integer,DataRecord>> s = _dataRecords.entrySet();

        Iterator<Map.Entry<Integer,DataRecord>> recordIter = s.iterator();
		
		while (recordIter.hasNext())
		{
			Map.Entry<Integer,DataRecord> m1 = recordIter.next();
			
			DataRecord record = m1.getValue();
			
			if (record.isDeletePending())
			{
				recordIter.remove();
			}
		}
	}
	
	public void rebuildIndex(String indexName)
	{
		if (_indexNames.contains(indexName))
		{
			String column = null;
			
			boolean indexFound = false;
			
			Iterator<Map.Entry<String,Index>> indexIter = _indexes.entrySet().iterator();
			
			// Remove old index
			while(indexIter.hasNext())
			{
				Map.Entry<String,Index> m = indexIter.next();
				
				if (indexName.compareTo(m.getValue().getIndexName()) == 0)
				{
					indexFound = true;
					column = m.getValue().getColumnName();
					indexIter.remove();
					break;
				}
			}
			
			if (indexFound)
			{
				// Remove index name from list of indexes, createIndex will add it back in.
				_indexNames.remove(indexName);

				// Rebuild index - this will only add records not tagged as delete
				createIndex(indexName, column);
			}
		}
	}
	
	private class FileIterator implements Iterator<Integer>
	{
		DataRecord _lastReturnedRecord;
		DataRecord _nextRecordToReturn;
		
		Iterator<Map.Entry<Integer,DataRecord>> _iter;
		
		int _expectedModCount;
		
		public FileIterator()
		{
			_iter = _dataRecords.entrySet().iterator();
			
			_lastReturnedRecord = null;
			_nextRecordToReturn = null;
			
			_expectedModCount = _modCount;
		}
		
		public boolean hasNext()
		{	
			if (_nextRecordToReturn != null)
			{
				if (!_nextRecordToReturn.isDeletePending())
				{
					return true;
				}
				else
				{
					throw new java.util.ConcurrentModificationException();
				}
			}
			else// _nextRecordToReturn == null
			{
				boolean validRecordFound = false;

				while (_iter.hasNext())
				{
					Map.Entry<Integer,DataRecord> m = _iter.next();

					_nextRecordToReturn = (DataRecord)m.getValue();

					if (!_nextRecordToReturn.isDeletePending())
					{
						validRecordFound = true;
						break;
					}
				}

				if (!validRecordFound)
				{
					_nextRecordToReturn = null;
				}
				
				return validRecordFound;
			}
		}
		
		public Integer next()
		{
			checkConcurrentModification();
			
			if (_nextRecordToReturn != null)
			{
				if (!_nextRecordToReturn.isDeletePending())
				{
					_lastReturnedRecord = _nextRecordToReturn;
					_nextRecordToReturn = null;
					return _lastReturnedRecord.getRecordId();
				}
				else
				{
					throw new java.util.ConcurrentModificationException();
				}
			}
			else
			{
				if (hasNext())
				{
					_lastReturnedRecord = _nextRecordToReturn;
					_nextRecordToReturn = null;
					return _lastReturnedRecord.getRecordId();
				}
				else
				{
					throw new java.util.NoSuchElementException();
				}
			}
		}
		
		public void remove()
		{
			checkConcurrentModification();
			
			if (_lastReturnedRecord != null)
			{
				if (!_lastReturnedRecord.isDeletePending())
				{
					_lastReturnedRecord.setDeletePending(true);
					_lastReturnedRecord = null;
					
					_expectedModCount++;
					_modCount++;
				}
			}
			else
			{
				throw new java.lang.IllegalStateException();
			}
		}
		
		private void checkConcurrentModification()
		{
			if (_expectedModCount != _modCount)
			{
				throw new java.util.ConcurrentModificationException();
			}
		}
	}
	
	public Iterator<Integer> iterator()
	{
		return new FileIterator();
	}
}

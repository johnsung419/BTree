package database;

import java.io.*;
import java.util.*;

public class Index implements Serializable
{
	private static final long serialVersionUID = 1L;
	private BTree _bTree;
	private String _indexName;
	private String _column;
	
	private DataFile _file;
	
	public Index(DataFile file, String indexName, String column)
	{
		_file = file;
		_indexName = indexName;
		_column = column;	
		_bTree = new BTree();
	}

	
	public void dumpIndex()
	{
		File indexFile = new File(_indexName);

		FileOutputStream fos = null;
		ObjectOutputStream out = null;

		try
		{
			fos = new FileOutputStream(indexFile);
			out = new ObjectOutputStream(fos);
			out.writeObject(this);

		}
		catch(IOException ex)
		{
			ex.printStackTrace();
		}
	}

	public String viewIndex()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("Index " + _indexName + " over column " + _column );
		sb.append("\n\n");
		sb.append(_bTree.printTree());

		System.out.println(new String(sb.toString()));
		
		return new String(sb.toString());
	}

	public void insertRecord(String key, DataRecord record)
	{
		_bTree.insert(key, record);
	}

	public String getColumnName()
	{
		return _column;
	}

	public String getIndexName()
	{
		return _indexName;
	}


	public Iterator<Integer> iterator(String key)
	{
		return new IndexIterator<Integer>(key);

	}
	
	@SuppressWarnings("hiding")
	private class IndexIterator<Integer> implements Iterator<Integer>
	{ 
		DataRecord _lastReturnedRecord;
		DataRecord _nextRecordToReturn;
		
		Iterator<DataRecord> _iter;
		
		int _expectedModCount;

		private IndexIterator(String key)
		{
			List<DataRecord> list = (List<DataRecord>) _bTree.getRecordList(key);
			
			if (list != null)
			{
				_iter = list.iterator();
			}
			else
			{
				_iter = null;
			}
			
			_lastReturnedRecord = null;
			_nextRecordToReturn = null;
			
			_expectedModCount = _file.getModCount();
		}


		@Override
		public boolean hasNext() 
		{
			if (_iter != null)
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
				else
				{
					boolean validRecordFound = false;

					while (_iter.hasNext())
					{
						_nextRecordToReturn = (DataRecord)_iter.next();

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
			else
			{
				return false;
			}
		}

		@Override
		public Integer next() 
		{
			checkConcurrentModification();
			
			if (_nextRecordToReturn != null)
			{
				if (!_nextRecordToReturn.isDeletePending())
				{
					_lastReturnedRecord = _nextRecordToReturn;
					_nextRecordToReturn = null;
					return (Integer)_lastReturnedRecord.getRecordId();
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
					return (Integer)_lastReturnedRecord.getRecordId();
				}
				else
				{
					throw new java.util.NoSuchElementException();
				}
			}
		}

		@Override
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
					_file.incrementModCount();
				}
			}
			else
			{
				throw new java.lang.IllegalStateException();
			}
		}
		
		private void checkConcurrentModification()
		{
			if (_expectedModCount != _file.getModCount())
			{
				throw new java.util.ConcurrentModificationException();
			}
		}
	}
}

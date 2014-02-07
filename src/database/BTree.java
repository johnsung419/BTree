package database;

import java.io.Serializable;
import java.util.*;

public class BTree implements Serializable
{
	private final int ORDER = 8;
	
	private static final long serialVersionUID = 8128671465444525120L;
	
	private class Node implements Serializable
	{
		private static final long serialVersionUID = 5869161159086543508L;
		
		String  _keys[];
		Object  _children[];
		int     _numKeys;
		boolean _isLeaf;
		
		private Node()
		{
			_keys     = new String[ORDER];
			_children = new Object[ORDER+1];
			_isLeaf   = true;
			_numKeys  = 0;
			
			for (int i = 0; i < ORDER; i++)
			{
				_keys[i]     = null;
				_children[i] = null;
			}
			_children[ORDER] = null;
		}
	}
	
	Node _root;
	
	// Temporary results from operations:
	Node   _newChild;
	String _newKey;
	
	public BTree()
	{
		_root = null;
	}
	
	public List<DataRecord> getRecordList(String key)
	{
		return getRecordList(_root, key);
	}
	
	private List<DataRecord> getRecordList(Node node, String key)
	{
		List<DataRecord> recordList = null;
		
		if (node._isLeaf)
		{
			for (int i = 0; i < node._numKeys; i++)
			{
				if (key.compareTo(node._keys[i]) == 0)
				{
					recordList = (List<DataRecord>)node._children[i];
					break;
				}
				else if (key.compareTo(node._keys[i]) < 0)
				{
					break;
				}
			}
			
			return recordList;
		}
		else
		{
			int index = 0;
			while ((index < node._numKeys) && (key.compareTo(node._keys[index]) >= 0))
			{
				index ++;
			}
			
			return getRecordList((Node)node._children[index], key);
		}
	}
	
	public String printTree()
	{
		StringBuilder stringBuilder = new StringBuilder("");
		printTreeInorder(_root, 0, stringBuilder);
		return new String(stringBuilder.toString());
	}
	
	private void printTreeInorder(Node node, int level, StringBuilder s)
	{
		if (node._isLeaf)
		{
			for (int i = 0; i < node._numKeys; i++)
			{
				List<DataRecord> recordList =
					(List<DataRecord>)node._children[i];
				
				Iterator<DataRecord> recordIter = recordList.iterator();
				
				while (recordIter.hasNext())
				{
					DataRecord record = recordIter.next();

					if (!record.isDeletePending())
					{
						for (int j = 0; j < level; j++)
						{
							s.append("\t");
						}

						s.append(node._keys[i]);
						s.append(" ");
						s.append(record.getRecordId());
						s.append("\n");
					}
				}
			}
		}
		else
		{
			for (int i = 0; i < node._numKeys; i++)
			{
				printTreeInorder((Node)node._children[i], level+1, s);
				
				for (int j = 0; j < level; j++)
				{
					s.append("\t");
				}
				s.append(node._keys[i]);
				s.append("\n");
			}
			printTreeInorder((Node)node._children[node._numKeys], level+1, s);
		}
	}
	
	public void insert(String key, DataRecord record)
	{
		_newChild = null;
		_newKey   = null;
		
		if (_root == null)
		{
			_root = new Node();
			_root._keys[0] = key;
			
			List<DataRecord> recordList = new LinkedList<DataRecord>();
			recordList.add(record);
			_root._children[0] = recordList;
			
			_root._numKeys = 1;
		}
		else
		{
			insert(_root, key, record);
			
			if (_newChild != null)
			{
				createNewRoot();
			}
		}
	}
	
	private void insert(Node node, String key, DataRecord record)
	{
		_newChild = null;
		_newKey   = null;
		
		if (node._isLeaf)
		{
			insertIntoLeaf(node, key, record);
		}
		else
		{
			int index = 0;
			while ((index < node._numKeys) && (key.compareTo(node._keys[index]) >= 0))
			{
				index ++;
			}
			
			insert((Node)node._children[index], key, record);
			
			if (_newChild != null)
			{
				if (node._numKeys < ORDER)
				{
					insertIntoNonfullInteriorNode(node);
				}
				else
				{
					splitInteriorNode(node);
				}
			}
		}
	}
	
	private void insertIntoNonfullInteriorNode(Node node)
	{
		// Locate the insertion index
		int index = 0;
		while ((index < node._numKeys) && (_newKey.compareTo(node._keys[index]) > 0))
		{
			index ++;
		}
		
		for (int tempIndex = node._numKeys; tempIndex > index; tempIndex--)
		{
			node._keys[tempIndex] = node._keys[tempIndex-1];
			node._children[tempIndex+1] = node._children[tempIndex];
		}
		
		node._keys[index] = _newKey;
		node._children[index+1] = _newChild;
		node._numKeys++;
		
		_newChild = null;
		_newKey   = null;
	}
	
	private void splitInteriorNode(Node node)
	{
		// Create temporary arrays to imitate an extended node
		String tempKeys[] = new String[ORDER+1];
		Object tempChildren[] = new Node[ORDER+2];
		
		// Copy all keys from the filled node to the temp key array
		for (int i = 0; i < node._numKeys; i++)
		{
			tempKeys[i] = node._keys[i];
		}
		
		// Copy all children from filled node to the temp children array
		for (int i = 0; i <= node._numKeys; i++)
		{
			tempChildren[i] = node._children[i];
		}
		
		// Locate the insertion index in the temp node
		int index = 0;
		while ((index < node._numKeys) && (_newKey.compareTo(node._keys[index]) > 0))
		{
			index ++;
		}
		
		// Insert the new key and child into the temp arrays
		for (int tempIndex = ORDER; tempIndex > index; tempIndex--)
		{
			tempKeys[tempIndex] = tempKeys[tempIndex-1];
			tempChildren[tempIndex+1] = tempChildren[tempIndex];
		}
		tempKeys[index] = _newKey;
		tempChildren[index+1] = _newChild;

		// Create a new interior node and copy the appropriate keys and
		// children to it
		Node rightNode = new Node();
		
		int offset = ORDER / 2 + 1;
		
		for (int i = 0; i < (ORDER / 2); i++)
		{
			rightNode._keys[i] = tempKeys[i+offset];
		}
		
		for (int i = 0; i < (ORDER / 2) + 1; i++)
		{
			rightNode._children[i] = tempChildren[i+offset];
		}
		
		rightNode._numKeys = ORDER / 2;
		rightNode._isLeaf = false;
		
		// Repopulate the left node, but first null everything
		for (int i = 0; i < ORDER; i++)
		{
			node._keys[i] = null;
			node._children[i] = null;
		}
		node._children[ORDER] = null;
		
		for (int i = 0; i < (ORDER / 2); i++)
		{
			node._keys[i] = tempKeys[i];
		}
		
		for (int i = 0; i < (ORDER / 2 + 1); i++)
		{
			node._children[i] = tempChildren[i];
		}
		
		node._numKeys = ORDER / 2;
		
		_newChild = rightNode;
		_newKey = tempKeys[ORDER / 2];
	}
	
	private void insertIntoLeaf(Node leaf, String key, DataRecord record)
	{
		boolean uniqueKey = true;
		
		for (int i = 0; i < leaf._numKeys; i++)
		{
			if (leaf._keys[i].compareTo(key) == 0)
			{
				uniqueKey = false;
				
				List<DataRecord> recordList =
					(List<DataRecord>)leaf._children[i];
				
				recordList.add(record);
				
				break;
			}
		}
		
		if (uniqueKey)
		{
			if (leaf._numKeys < ORDER)
			{
				insertIntoNonfullLeaf(leaf, key, record);
			}
			else
			{
				splitLeaf(leaf, key, record);
			}
		}
	}
	
	private void insertIntoNonfullLeaf(Node leaf, String key, DataRecord record)
	{
		// Locate the insertion index
		int index = 0;
		while ((index < leaf._numKeys) && (key.compareTo(leaf._keys[index]) > 0))
		{
			index ++;
		}
		
		for (int tempIndex = leaf._numKeys; tempIndex > index; tempIndex--)
		{
			leaf._keys[tempIndex] = leaf._keys[tempIndex-1];
			leaf._children[tempIndex] = leaf._children[tempIndex-1];
		}
		
		leaf._keys[index] = key;
		
		List<DataRecord> recordList = new LinkedList<DataRecord>();
		recordList.add(record);
		leaf._children[index] = recordList;
		
		leaf._numKeys++;
		
		_newChild = null;
		_newKey   = null;
	}
	
	private void splitLeaf(Node leaf, String key, DataRecord record)
	{
		Node rightLeaf = new Node();
		
		int indexOffset = ORDER / 2;
		
		// Move elements to new leaf and remove references from old leaf
		for (int i = 0; i < indexOffset; i++)
		{
			rightLeaf._keys[i] = leaf._keys[i+indexOffset];
			leaf._keys[i+indexOffset] = null;
			
			rightLeaf._children[i] = leaf._children[i+indexOffset];
			leaf._children[i+indexOffset] = null;
		}
		
		leaf._numKeys -= indexOffset;
		rightLeaf._numKeys = indexOffset;
		
		// Set up the leaf-to-leaf pointers
		rightLeaf._children[ORDER] = leaf._children[ORDER];
		leaf._children[ORDER] = rightLeaf;
		
		if (key.compareTo(rightLeaf._keys[0]) <= 0)
		{
			// Insert into left leaf, which is now non-full
			insertIntoNonfullLeaf(leaf, key, record);
		}
		else
		{
			// Insert into right leaf, which is non-full
			insertIntoNonfullLeaf(rightLeaf, key, record);
		}
		
		_newChild = rightLeaf;
		_newKey = rightLeaf._keys[0];
	}
	
	private void createNewRoot()
	{
		Node newRoot = new Node();
		newRoot._keys[0] = _newKey;
		newRoot._children[0] = _root;
		newRoot._children[1] = _newChild;
		newRoot._numKeys = 1;
		newRoot._isLeaf = false;
		
		_root = newRoot;
	}
}

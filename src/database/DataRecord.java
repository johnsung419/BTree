package database;

import java.io.Serializable;
import java.util.Map;

public class DataRecord implements Serializable
{
	private static final long serialVersionUID = -6429224810900461803L;

	private Integer _recordId;
	private Map<String,String> _values;
	private boolean _deleteFlag;
	
	public DataRecord(Integer recordId, Map<String,String> values)
	{
		_recordId = recordId;
		_values = values;
		_deleteFlag = false;
	}
	
	public Integer getRecordId()
	{
		return _recordId;
	}
	
	public Map<String,String> getValues()
	{
		return _values;
	}
	
	public boolean isDeletePending()
	{
		return _deleteFlag;
	}
	
	public void setDeletePending(boolean value)
	{
		_deleteFlag = value;
	}
}

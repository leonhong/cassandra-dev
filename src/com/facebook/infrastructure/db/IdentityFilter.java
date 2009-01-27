package com.facebook.infrastructure.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.SSTable;

public class IdentityFilter implements IFilter
{
    private boolean isDone_ = false;
	public boolean isDone()
	{
		return isDone_;
	}

	public ColumnFamily filter(String cfString, ColumnFamily columnFamily)
	{
    	String[] values = RowMutation.getColumnAndColumnFamily(cfString);
    	if( columnFamily == null )
    		return columnFamily;
		String cfName = columnFamily.name();
		if ( values.length == 2 && !DatabaseDescriptor.getColumnType(cfName).equals("Super") )
		{
			Collection<IColumn> columns = columnFamily.getAllColumns();
			if(columns.size() >= 1)
				isDone_ = true;
		}
		if ( values.length == 3 && DatabaseDescriptor.getColumnType(cfName).equals("Super"))
		{
    		Collection<IColumn> columns = columnFamily.getAllColumns();
    		for(IColumn column : columns)
    		{
    			SuperColumn superColumn = (SuperColumn)column;
        		Collection<IColumn> subColumns = superColumn.getSubColumns();
        		if( subColumns.size() >= 1 )
        			isDone_ = true;
    		}
		}
		return columnFamily;
	}

	public IColumn filter(IColumn column, DataInputStream dis)
			throws IOException
	{
		// TODO Auto-generated method stub
		return column;
	}

	public DataInputBuffer next(String key, String cf, SSTable ssTable)
			throws IOException
	{
		return ssTable.next(key, cf);
	}

	public void setDone()
	{
		isDone_ = true;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub

	}

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.infrastructure.db;

import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.utils.FBUtilities;
import org.apache.log4j.Logger;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public final class ColumnFamily
{
    private static ColumnFamilySerializer serializer_;
    public static final short utfPrefix_ = 2;
    /* The column serializer for this Column Family. Create based on config. */

    private static Logger logger_ = Logger.getLogger( ColumnFamily.class );
    private static Map<String, String> columnTypes_ = new HashMap<String, String>();
    private static Map<String, String> indexTypes_ = new HashMap<String, String>();

    static
    {
        serializer_ = new ColumnFamilySerializer();
        /* TODO: These are the various column types. Hard coded for now. */
        columnTypes_.put("Standard", "Standard");
        columnTypes_.put("Super", "Super");

        indexTypes_.put("None", "None");
        indexTypes_.put("Name", "Name");
        indexTypes_.put("Time", "Time");
    }

    public static ColumnFamilySerializer serializer()
    {
        return serializer_;
    }

    /*
     * This method returns the serializer whose methods are
     * preprocessed by a dynamic proxy.
    */
    public static ICompactSerializer2<ColumnFamily> serializerWithIndexes()
    {
        return (ICompactSerializer2<ColumnFamily>)Proxy.newProxyInstance( ColumnFamily.class.getClassLoader(), new Class[]{ICompactSerializer2.class}, new ColumnIndexInvocationHandler<ColumnFamily>(serializer_) );
    }

    public static String getColumnType(String key)
    {
    	if ( key == null )
    		return columnTypes_.get("Standard");
    	return columnTypes_.get(key);
    }

    public static String getColumnIndexProperty(String columnIndexProperty)
    {
    	if ( columnIndexProperty == null )
    		return indexTypes_.get("None");
    	return indexTypes_.get(columnIndexProperty);
    }

    private transient AbstractColumnFactory columnFactory_;

    private String name_;

    private transient ICompactSerializer2<IColumn> columnSerializer_;
    private long markedForDeleteAt = Long.MIN_VALUE;
    private AtomicInteger size_ = new AtomicInteger(0);
    private EfficientBidiMap columns_;

    private Comparator<IColumn> columnComparator_;

	private Comparator<IColumn> getColumnComparator(String cfName, String columnType)
	{
		if(columnComparator_ == null)
		{
			/*
			 * if this columnfamily has supercolumns or there is an index on the column name,
			 * then sort by name
			*/
			if("Super".equals(columnType) || DatabaseDescriptor.isNameIndexEnabled(cfName))
			{
				columnComparator_ = ColumnComparatorFactory.getComparator(ColumnComparatorFactory.ComparatorType.NAME);
			}
			/* if this columnfamily has simple columns, and no index on name sort by timestamp */
			else
			{
				columnComparator_ = ColumnComparatorFactory.getComparator(ColumnComparatorFactory.ComparatorType.TIMESTAMP);
			}
		}

		return columnComparator_;
	}

    /* CTOR for JAXB */
    private ColumnFamily()
    {
    }

    public ColumnFamily(String cf)
    {
        name_ = cf;
        createColumnFactoryAndColumnSerializer();
    }

    public ColumnFamily(String cf, String columnType)
    {
        name_ = cf;
        createColumnFactoryAndColumnSerializer(columnType);
    }

    void createColumnFactoryAndColumnSerializer(String columnType)
    {
        if ( columnFactory_ == null )
        {
            columnFactory_ = AbstractColumnFactory.getColumnFactory(columnType);
            columnSerializer_ = columnFactory_.createColumnSerializer();
            if(columns_ == null)
                columns_ = new EfficientBidiMap(getColumnComparator(name_, columnType));
        }
    }

    void createColumnFactoryAndColumnSerializer()
    {
    	String columnType = DatabaseDescriptor.getColumnFamilyType(name_);
        if ( columnType == null )
        {
        	List<String> tables = DatabaseDescriptor.getTables();
        	if ( tables.size() > 0 )
        	{
        		String table = tables.get(0);
        		columnType = Table.open(table).getColumnFamilyType(name_);
        	}
        }
        createColumnFactoryAndColumnSerializer(columnType);
    }

    ColumnFamily cloneMe()
    {
    	ColumnFamily cf = new ColumnFamily(name_);
    	cf.markedForDeleteAt = markedForDeleteAt;
    	cf.columns_ = columns_.cloneMe();
    	return cf;
    }

    public String name()
    {
        return name_;
    }

    /*
     *  We need to go through each column
     *  in the column family and resolve it before adding
    */
    void addColumns(ColumnFamily cf)
    {
        for (IColumn column : cf.getColumns().values())
        {
            addColumn(column.name(), column);
        }
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        createColumnFactoryAndColumnSerializer();
    	return columnSerializer_;
    }

    public IColumn createColumn(String name)
    {
    	IColumn column = columnFactory_.createColumn(name);
    	addColumn(column.name(), column);
        return column;
    }

//    int getColumnCount()
//    {
//    	int count = 0;
//    	Set<IColumn> columns = columns_.getSortedColumns();
//    	if( columns != null )
//    	{
//	    	for(IColumn column: columns)
//	    	{
//	    		count = count + column.getObjectCount();
//	    	}
//    	}
//    	return count;
//    }

    int getColumnCount()
    {
    	int count = 0;
    	Map<String, IColumn> columns = columns_.getColumns();
    	if( columns != null )
    	{
    		if(!isSuper())
    		{
    			count = columns.size();
    		}
    		else
    		{
    			Collection<IColumn> values = columns.values();
		    	for(IColumn column: values)
		    	{
		    		count += column.getObjectCount();
		    	}
    		}
    	}
    	return count;
    }

    public boolean isSuper() {
        return DatabaseDescriptor.getColumnType(name_).equals("Super");
    }

    public IColumn createColumn(String name, byte[] value)
    {
    	IColumn column = columnFactory_.createColumn(name, value);
    	addColumn(column.name(), column);
        return column;
    }

	public IColumn createColumn(String name, byte[] value, long timestamp)
	{
		IColumn column = columnFactory_.createColumn(name, value, timestamp);
		addColumn(column.name(), column);
        return column;
    }

    void clear()
    {
    	columns_.clear();
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column .
    */
    void addColumn(String name, IColumn column)
    {
        IColumn oldColumn = columns_.get(name);
        if ( oldColumn != null )
        {
            if( oldColumn.putColumn(column))
            {
                size_.set(oldColumn.size());
            }
        }
        else
        {
            size_.addAndGet(column.size());
            columns_.put(name, column);
        }
    }

    public IColumn getColumn(String name)
    {
        return columns_.get( name );
    }

    public Collection<IColumn> getAllColumns()
    {
        return columns_.getSortedColumns();
    }

    Map<String, IColumn> getColumns()
    {
        return columns_.getColumns();
    }

    public void remove(String columnName)
    {
    	columns_.remove(columnName);
    }

    void delete(long timestamp)
    {
        markedForDeleteAt = timestamp;
    }

    /* TODO this is tempting to misuse: the goal is to send a timestamp to the nodes w/ the data,
       saying to delete all applicable columns.  other than that it has no meaning!
       Thus, this should really be part of the RowMutation message. */
    public boolean isMarkedForDelete()
    {
        return markedForDeleteAt > Long.MIN_VALUE;
    }

    /*
     * This is used as oldCf.merge(newCf). Basically we take the newCf
     * and merge it into the oldCf.
    */
    void merge(ColumnFamily columnFamily)
    {
        Map<String, IColumn> columns = columnFamily.getColumns();
        Set<String> cNames = columns.keySet();

        for ( String cName : cNames )
        {
            columns_.put(cName, columns.get(cName));
        }
    }

    /*
     * This function will repair a list of columns
     * If there are any columns in the external list which are not present
     * internally then they are added ( this might have to change depending on
     * how we implement delete repairs).
     * Also if there are any columns in teh internal and not in the external
     * they are kept intact.
     * Else the one with the greatest timestamp is considered latest.
     */
    void repair(ColumnFamily columnFamily)
    {
        Map<String, IColumn> columns = columnFamily.getColumns();
        Set<String> cNames = columns.keySet();

        for ( String cName : cNames )
        {
        	IColumn columnInternal = columns_.get(cName);
        	IColumn columnExternal = columns.get(cName);

        	if( columnInternal == null )
        	{
        		if(isSuper())
        		{
        			columnInternal = new SuperColumn(columnExternal.name());
        			columns_.put(cName, columnInternal);
        		}
                else
        		{
        			columnInternal = columnExternal;
        			columns_.put(cName, columnInternal);
        		}
        	}
       		columnInternal.repair(columnExternal);
        }
    }


    /*
     * This function will calculate the differnce between 2 column families
     * the external input is considered the superset of internal
     * so there are no deletes in the diff.
     */
    ColumnFamily diff(ColumnFamily columnFamily)
    {
    	ColumnFamily cfDiff = new ColumnFamily(columnFamily.name());
        Map<String, IColumn> columns = columnFamily.getColumns();
        Set<String> cNames = columns.keySet();

        for ( String cName : cNames )
        {
        	IColumn columnInternal = columns_.get(cName);
        	IColumn columnExternal = columns.get(cName);
        	if( columnInternal == null )
        	{
        		cfDiff.addColumn(cName, columnExternal);
        	}
        	else
        	{
            	IColumn columnDiff = columnInternal.diff(columnExternal);
        		if(columnDiff != null)
        		{
        			cfDiff.addColumn(cName, columnDiff);
        		}
        	}
        }
        if(cfDiff.getColumns().size() != 0)
        	return cfDiff;
        else
        	return null;
    }

    int size()
    {
        if ( size_.get() == 0 )
        {
            Set<String> cNames = columns_.getColumns().keySet();
            for ( String cName : cNames )
            {
                size_.addAndGet(columns_.get(cName).size());
            }
        }
        return size_.get();
    }

    public int hashCode()
    {
        return name().hashCode();
    }

    public boolean equals(Object o)
    {
        if ( !(o instanceof ColumnFamily) )
            return false;
        ColumnFamily cf = (ColumnFamily)o;
        return name().equals(cf.name());
    }

    public String toString()
    {
    	StringBuilder sb = new StringBuilder();
    	sb.append(name_);
    	sb.append(":");
    	sb.append(isMarkedForDelete());
    	sb.append(":");
    	Collection<IColumn> columns = getAllColumns();
        sb.append(columns.size());
        sb.append(":");

        for ( IColumn column : columns )
        {
            sb.append(column.toString());
        }
        sb.append(":");
    	return sb.toString();
    }

    public byte[] digest()
    {
    	Set<IColumn> columns = columns_.getSortedColumns();
    	byte[] xorHash = new byte[0];
    	byte[] tmpHash = new byte[0];
    	for(IColumn column : columns)
    	{
    		if(xorHash.length == 0)
    		{
    			xorHash = column.digest();
    		}
    		else
    		{
    			tmpHash = column.digest();
    			xorHash = FBUtilities.xor(xorHash, tmpHash);
    		}
    	}
    	return xorHash;
    }

    public long getMarkedForDeleteAt() {
        return markedForDeleteAt;
    }
}

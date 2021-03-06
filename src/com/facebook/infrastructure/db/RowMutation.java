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

import com.facebook.infrastructure.io.ICompactSerializer;
import com.facebook.infrastructure.service.*;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.utils.FBUtilities;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class RowMutation implements Serializable
{      
	private static ICompactSerializer<RowMutation> serializer_;
    public static final String HINT = "HINT";

    static
    {
        serializer_ = new RowMutationSerializer();
    }

    static ICompactSerializer<RowMutation> serializer()
    {
        return serializer_;
    }
        
    private String table_;
    private String key_;       
    protected Map<String, ColumnFamily> modifications_ = new HashMap<String, ColumnFamily>();       

    /* Ctor for JAXB */
    private RowMutation()
    {
    }
    
    public RowMutation(String table, String key)
    {
        table_ = table;
        key_ = key;
    }
    
    public RowMutation(String table, Row row)
    {
        table_ = table;
        key_ = row.key();
        for (ColumnFamily cf : row.getColumnFamilies()) {
        	add(cf);
        }
    }

    protected RowMutation(String table, String key, Map<String, ColumnFamily> modifications)
    {
    	table_ = table;
    	key_ = key;
    	modifications_ = modifications;
    }
    
    public Message makeRowMutationMessage() throws IOException
    {
        return makeRowMutationMessage(StorageService.mutationVerbHandler_);
    }

    public Message makeRowMutationMessage(String verbHandlerName) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        serializer().serialize(this, dos);
        EndPoint local = StorageService.getLocalStorageEndPoint();
        EndPoint from = ( local != null ) ? local : new EndPoint(FBUtilities.getHostName(), 7000);
        Message message = new Message(from, StorageService.mutationStage_, verbHandlerName, bos.toByteArray());
        return message;
    }

    public static String[] getColumnAndColumnFamily(String cf)
    {
        return cf.split(":");
    }
    
    String table()
    {
        return table_;
    }
    
    public String key()
    {
        return key_;
    }
    
    void addHints(String hint) throws IOException, ColumnFamilyNotDefinedException
    {        
        String cfName = Table.hints_ + ":" + hint;
        add(cfName, ArrayUtils.EMPTY_BYTE_ARRAY, 0);
    }
    
    /*
     * Specify a column family name and the corresponding column
     * family object. 
     * param @ cf - column family name
     * param @ columnFamily - the column family.
    */
    public void add(ColumnFamily columnFamily)
    {
        if (modifications_.containsKey(columnFamily.name())) {
            throw new IllegalArgumentException("ColumnFamily " + columnFamily.name() + " is already being modified");
        }
        modifications_.put(columnFamily.name(), columnFamily);
    }

    /*
     * Specify a column name and a corresponding value for
     * the column. Column name is specified as <column family>:column.
     * This will result in a ColumnFamily associated with
     * <column family> as name and a Column with <column>
     * as name. The columan can be further broken up 
     * as super column name : columnname  in case of super columns
     * 
     * param @ cf - column name as <column family>:<column>
     * param @ value - value associated with the column
     * param @ timestamp - ts associated with this data.
    */
    public void add(String columnFamilyColumn, byte[] value, long timestamp)
    {        
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamilyColumn);
        String cfName = values[0];

        if ( values.length == 0 || values.length == 1 || values.length > 3 )
            throw new IllegalArgumentException("Column Family " + columnFamilyColumn + " in invalid format. Must be in <column family>:<column> format.");
        
        ColumnFamily columnFamily = modifications_.get(cfName);
        if( values.length == 2 )
        {
            if ( columnFamily == null )
            {
            	columnFamily = new ColumnFamily(cfName, ColumnFamily.getColumnType("Standard"));
            } else {
                if (columnFamily.getColumn(values[1]) != null) {
                    throw new IllegalArgumentException(columnFamilyColumn + " is already being modified");
                }
            }
        	columnFamily.addColumn(values[1], value, timestamp);
        }
        if( values.length == 3 )
        {
            if ( columnFamily == null )
            {
            	columnFamily = new ColumnFamily(cfName, ColumnFamily.getColumnType("Super"));
            } else {
                IColumn sc = columnFamily.getColumn(values[1]);
                if (sc != null && ((SuperColumn)sc).getSubColumn(values[2]) != null) {
                    throw new IllegalArgumentException(columnFamilyColumn + " is already being modified");
                }
            }
        	columnFamily.addColumn(values[1]+ ":" + values[2], value, timestamp);
        }
        modifications_.put(cfName, columnFamily);
    }
    
    /*
     * Specify a column name to be deleted. Column name is
     * specified as <column family>:column. This will result
     * in a ColumnFamily associated with <column family> as
     * name and perhaps Column with <column> as name being
     * marked as deleted.
     * param @ cf - column name as <column family>:<column>     
    */
    public void delete(String columnFamilyColumn, long timestamp)
    {        
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamilyColumn);
        String cfName = values[0];
        if (modifications_.containsKey(cfName)) {
            throw new IllegalArgumentException("ColumnFamily " + cfName + " is already being modified");
        }

        if ( values.length == 0 || values.length > 3 )
            throw new IllegalArgumentException("Column Family " + columnFamilyColumn + " in invalid format. Must be in <column family>:<column> format.");

        ColumnFamily columnFamily = modifications_.get(cfName);
        if ( columnFamily == null )
            columnFamily = new ColumnFamily(cfName);
        if(values.length == 2 )
        {
            if (columnFamily.isSuper()) {
                IColumn c = columnFamily.addColumn(values[1]);
                ((SuperColumn)c).markForDeleteAt(timestamp);
            } else {
                IColumn c = columnFamily.addColumn(values[1], ArrayUtils.EMPTY_BYTE_ARRAY, timestamp, true);
            }
        }
        else if(values.length == 3 )
        {
            IColumn c = columnFamily.addColumn(values[1] + ":" + values[2], ArrayUtils.EMPTY_BYTE_ARRAY, timestamp, true);
        }
        else {
            assert values.length == 1;
            columnFamily.delete(timestamp);
        }
        modifications_.put(cfName, columnFamily);
    }
    
    /* 
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    public void apply() throws IOException, ColumnFamilyNotDefinedException
    {  
        Row row = new Row(key_);
        apply(row);
    }
    
    /* 
     * Allows RowMutationVerbHandler to optimize by re-using a single Row object.
    */
    void apply(Row emptyRow) throws IOException, ColumnFamilyNotDefinedException
    {
        assert emptyRow.getColumnFamilies().size() == 0;
        Table table = Table.open(table_);
        for (String cfName : modifications_.keySet())
        {        
            if ( !table.isValidColumnFamily(cfName) )
                throw new ColumnFamilyNotDefinedException("Column Family " + cfName + " has not been defined.");
            emptyRow.addColumnFamily( modifications_.get(cfName) );
        }
        table.apply(emptyRow);
    }
    
    /* 
     * This is equivalent to calling commit. Applies the changes to
     * to the table that is obtained by calling Table.open().
    */
    void load(Row row) throws IOException, ColumnFamilyNotDefinedException, ExecutionException, InterruptedException {
        Table table = Table.open(table_);        
        Set<String> cfNames = modifications_.keySet();
        for (String cfName : cfNames )
        {        
            if ( !table.isValidColumnFamily(cfName) )
                throw new ColumnFamilyNotDefinedException("Column Family " + cfName + " has not been defined.");
            row.addColumnFamily( modifications_.get(cfName) );            
        }
        table.load(row);
    }

    public static RowMutation getRowMutation(batch_mutation_t batchMutation) {
        RowMutation rm = new RowMutation(batchMutation.table,
                batchMutation.key.trim());
        for (String cfname : batchMutation.cfmap.keySet()) {
            List<column_t> list = batchMutation.cfmap.get(cfname);
            for (column_t columnData : list) {
                rm.add(cfname + ":" + columnData.columnName,
                       columnData.value.getBytes(), columnData.timestamp);

            }
        }
        return rm;
    }

    public static RowMutation getRowMutation(batch_mutation_super_t batchMutationSuper) {
        RowMutation rm = new RowMutation(batchMutationSuper.table,
                                         batchMutationSuper.key.trim());
        Set keys = batchMutationSuper.cfmap.keySet();
        Iterator keyIter = keys.iterator();
        while (keyIter.hasNext()) {
            Object key = keyIter.next(); // Get the next key.
            List<superColumn_t> list = batchMutationSuper.cfmap.get(key);
            for (superColumn_t superColumnData : list) {
                if (superColumnData.columns.size() != 0) {
                    for (column_t columnData : superColumnData.columns) {
                        rm.add(key.toString() + ":" + superColumnData.name + ":" + columnData.columnName,
                               columnData.value.getBytes(), columnData.timestamp);
                    }
                } else {
                    rm.add(key.toString() + ":" + superColumnData.name, ArrayUtils.EMPTY_BYTE_ARRAY, 0);
                }
            }
        }
        return rm;
    }

    public String toString() {
        return "RowMutation(" +
               "key='" + key_ + '\'' +
               ", modifications=[" + StringUtils.join(modifications_.values(), ", ") + "]" +
               ')';
    }
}

class RowMutationSerializer implements ICompactSerializer<RowMutation>
{
	private void freezeTheMaps(Map<String, ColumnFamily> map, DataOutputStream dos) throws IOException
	{
		int size = map.size();
        dos.writeInt(size);
        if ( size > 0 )
        {   
            Set<String> keys = map.keySet();
            for( String key : keys )
            {                
            	dos.writeUTF(key);
                ColumnFamily cf = map.get(key);
                if ( cf != null )
                {
                    ColumnFamily.serializer().serialize(cf, dos);
                }                
            }
        }
	}
	
	public void serialize(RowMutation rm, DataOutputStream dos) throws IOException
	{
		dos.writeUTF(rm.table());
		dos.writeUTF(rm.key());
		
		/* serialize the modifications_ in the mutation */
        freezeTheMaps(rm.modifications_, dos);
	}
	
	private Map<String, ColumnFamily> defreezeTheMaps(DataInputStream dis) throws IOException
	{
		Map<String, ColumnFamily> map = new HashMap<String, ColumnFamily>();
        int size = dis.readInt();
        for ( int i = 0; i < size; ++i )
        {
            String key = dis.readUTF();  
            ColumnFamily cf = ColumnFamily.serializer().deserialize(dis);
            map.put(key, cf);            
        }
        return map;
	}
	
    public RowMutation deserialize(DataInputStream dis) throws IOException
    {
    	String table = dis.readUTF();
    	String key = dis.readUTF();
       	Map<String, ColumnFamily> modifications = defreezeTheMaps(dis);
    	return new RowMutation(table, key, modifications);
    }
}

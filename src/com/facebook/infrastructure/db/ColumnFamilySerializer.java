package com.facebook.infrastructure.db;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.util.Collection;

public class ColumnFamilySerializer implements ICompactSerializer2<ColumnFamily>
{
    /*
	 * We are going to create indexes, and write out that information as well. The format
	 * of the data serialized is as follows.
	 *
	 * 1) Without indexes:
     *  // written by the data
	 * 	<boolean false (index is not present)>
	 * 	<column family id>
	 * 	<is marked for delete>
	 * 	<total number of columns>
	 * 	<columns data>

	 * 	<boolean true (index is present)>
	 *
	 *  This part is written by the column indexer
	 * 	<size of index in bytes>
	 * 	<list of column names and their offsets relative to the first column>
	 *
	 *  <size of the cf in bytes>
	 * 	<column family id>
	 * 	<is marked for delete>
	 * 	<total number of columns>
	 * 	<columns data>
	*/
    public void serialize(ColumnFamily columnFamily, DataOutputStream dos) throws IOException
    {
    	Collection<IColumn> columns = columnFamily.getAllColumns();

        /* write the column family id */
        dos.writeUTF(columnFamily.name());
        /* write if this cf is marked for delete */
        dos.writeLong(columnFamily.getMarkedForDeleteAt());

        if (!columnFamily.isMarkedForDelete()) {
            /* write the size is the number of columns */
            dos.writeInt(columns.size());

            /* write the column data */
            for ( IColumn column : columns )
            {
                columnFamily.getColumnSerializer().serialize(column, dos);
            }
        }
    }

    /*
     * Use this method to create a bare bones Column Family. This column family
     * does not have any of the Column information.
    */
    private ColumnFamily defreezeColumnFamily(DataInputStream dis) throws IOException
    {
        String name = dis.readUTF();
        ColumnFamily cf = new ColumnFamily(name);
        cf.delete(dis.readLong());
        return cf;
    }

    public ColumnFamily deserialize(DataInputStream dis) throws IOException
    {
        if ( dis.available() == 0 )
            return null;

        ColumnFamily cf = defreezeColumnFamily(dis);
        if ( !cf.isMarkedForDelete() ) {
            int size = dis.readInt();
            IColumn column = null;
            for ( int i = 0; i < size; ++i )
            {
                column = cf.getColumnSerializer().deserialize(dis);
                if(column != null)
                {
                    cf.addColumn(column);
                }
            }
        }
        return cf;
    }

    /*
     * This version of deserialize is used when we need a specific set if columns for
     * a column family specified in the name cfName parameter.
    */
    public ColumnFamily deserialize(DataInputStream dis, IFilter filter) throws IOException
    {
        if ( dis.available() == 0 )
            return null;

        ColumnFamily cf = defreezeColumnFamily(dis);
        if ( !cf.isMarkedForDelete() )
        {
            int size = dis.readInt();
        	IColumn column = null;
            for ( int i = 0; i < size; ++i )
            {
            	column = cf.getColumnSerializer().deserialize(dis, filter);
            	if(column != null)
            	{
            		cf.addColumn(column);
            		column = null;
            		if(filter.isDone())
            		{
            			break;
            		}
            	}
            }
        }
        return cf;
    }

    /*
     * Deserialize a particular column or super column or the entire columnfamily given a : seprated name
     * name could be of the form cf:superColumn:column  or cf:column or cf
     */
    public ColumnFamily deserialize(DataInputStream dis, String name, IFilter filter) throws IOException
    {
        if ( dis.available() == 0 )
            return null;

        String[] names = RowMutation.getColumnAndColumnFamily(name);
        String columnName = "";
        if ( names.length == 1 )
            return deserialize(dis, filter);
        if( names.length == 2 )
            columnName = names[1];
        if( names.length == 3 )
            columnName = names[1]+ ":" + names[2];

        ColumnFamily cf = defreezeColumnFamily(dis);
        if ( !cf.isMarkedForDelete() )
        {
            /* read the number of columns */
            int size = dis.readInt();
            for ( int i = 0; i < size; ++i )
            {
	            IColumn column = cf.getColumnSerializer().deserialize(dis, columnName, filter);
	            if ( column != null )
	            {
	                cf.addColumn(column);
	                break;
	            }
            }
        }
        return cf;
    }

    public void skip(DataInputStream dis) throws IOException
    {
        throw new UnsupportedOperationException("This operation is not yet supported.");
    }
}

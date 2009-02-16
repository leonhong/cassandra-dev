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
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.service.StorageService;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ReadParameters
{
    private static ICompactSerializer<ReadParameters> serializer_ = new ReadMessageSerializer();

    static ICompactSerializer<ReadParameters> serializer()
    {
        return serializer_;
    }

    private static List<String> EMPTY_COLUMNS = Arrays.asList(new String[0]);
    
    public static Message makeReadMessage(ReadParameters readMessage) throws IOException
    {
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        ReadParameters.serializer().serialize(readMessage, dos);
        Message message = new Message(StorageService.getLocalStorageEndPoint(), StorageService.readStage_, StorageService.readVerbHandler_, bos.toByteArray());         
        return message;
    }
    
    public final String table;
    
    public final String key;
    
    public final String columnFamily_column;
    
    public final int start;

    public final int count;
    
    public final long sinceTimestamp;

    private final List<String> columns;
    
    private boolean isDigestQuery_ = false;

    public ReadParameters(String table, String key, String columnFamily_column, int start, int count, long sinceTimestamp, List<String> columns) {
        this.table = table;
        this.key = key;
        this.columnFamily_column = columnFamily_column;
        this.start = start;
        this.count = count;
        this.sinceTimestamp = sinceTimestamp;
        this.columns = columns;
    }

    public ReadParameters(String table, String key)
    {
        this(table, key, null, -1, -1, -1, EMPTY_COLUMNS);
    }

    public ReadParameters(String table, String key, String columnFamily_column)
    {
        this(table, key, columnFamily_column, -1, -1, -1, EMPTY_COLUMNS);
    }
    
    public ReadParameters(String table, String key, String columnFamily_column, List<String> columns)
    {
        this(table, key, columnFamily_column, -1, -1, -1, columns);
    }
    
    public ReadParameters(String table, String key, String columnFamily_column, int start, int count)
    {
        this(table, key, columnFamily_column, start, count, -1, EMPTY_COLUMNS);
    }

    public ReadParameters(String table, String key, String columnFamily_column, long sinceTimestamp)
    {
        this(table, key, columnFamily_column, -1, -1, sinceTimestamp, EMPTY_COLUMNS);
    }

    public boolean isDigestQuery()
    {
    	return isDigestQuery_;
    }
    
    public void setIsDigestQuery(boolean isDigestQuery)
    {
    	isDigestQuery_ = isDigestQuery;
    }
    
    public List<String> getColumnNames()
    {
    	return columns;
    }

    public ReadParameters copy() {
        return new ReadParameters(table, key, columnFamily_column, start, count, sinceTimestamp, columns);
    }

    public Row getRow(Table table) throws IOException, ColumnFamilyNotDefinedException {
        if (columns != EMPTY_COLUMNS) {
            return table.getRow(key, columnFamily_column, columns);
        }

        if (sinceTimestamp > 0) {
            return table.getRow(key, columnFamily_column, sinceTimestamp);
        }

		if( start >= 0 && count < Integer.MAX_VALUE)
		{
			return table.getRow(key, columnFamily_column, start, count);
		}
        return table.getRow(key, columnFamily_column);
    }

    public String toString() {
        return "ReadParameters{" +
               "sinceTimestamp=" + sinceTimestamp +
               ", key='" + key + '\'' +
               ", columnFamily_column='" + columnFamily_column + '\'' +
               ", start=" + start +
               ", count=" + count +
               '}';
    }
}

class ReadMessageSerializer implements ICompactSerializer<ReadParameters>
{
	public void serialize(ReadParameters rm, DataOutputStream dos) throws IOException
	{
		dos.writeUTF(rm.table);
		dos.writeUTF(rm.key);
		dos.writeUTF(rm.columnFamily_column);
		dos.writeInt(rm.start);
		dos.writeInt(rm.count);
		dos.writeLong(rm.sinceTimestamp);
		dos.writeBoolean(rm.isDigestQuery());
		List<String> columns = rm.getColumnNames();
		dos.writeInt(columns.size());
		if ( columns.size() > 0 )
		{
			for ( String column : columns )
			{
				dos.writeInt(column.getBytes().length);
				dos.write(column.getBytes());
			}
		}
	}
	
    public ReadParameters deserialize(DataInputStream dis) throws IOException
    {
		String table = dis.readUTF();
		String key = dis.readUTF();
		String columnFamily_column = dis.readUTF();
		int start = dis.readInt();
		int count = dis.readInt();
		long sinceTimestamp = dis.readLong();
		boolean isDigest = dis.readBoolean();
		
		int size = dis.readInt();
		List<String> columns = new ArrayList<String>();		
		for ( int i = 0; i < size; ++i )
		{
			byte[] bytes = new byte[dis.readInt()];
			dis.readFully(bytes);
			columns.add( new String(bytes) );
		}
		ReadParameters rm = null;
		if ( columns.size() > 0 )
		{
			rm = new ReadParameters(table, key, columnFamily_column, columns);
		}
		if( sinceTimestamp > 0 )
		{
			rm = new ReadParameters(table, key, columnFamily_column, sinceTimestamp);
		}
		else
		{
			rm = new ReadParameters(table, key, columnFamily_column, start, count);
		}
		rm.setIsDigestQuery(isDigest);
    	return rm;
    }
}


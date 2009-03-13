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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BasicUtilities;
import org.apache.cassandra.utils.FBUtilities;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class DBManager
{
    private static DBManager dbMgr_;
    private static Lock lock_ = new ReentrantLock();

    public static DBManager instance() throws Throwable
    {
        if ( dbMgr_ == null )
        {
            lock_.lock();
            try
            {
                if ( dbMgr_ == null )
                    dbMgr_ = new DBManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return dbMgr_;
    }

    public static class StorageMetadata
    {
        private Token myToken;
        private int generation_;

        StorageMetadata(Token storageId, int generation)
        {
            myToken = storageId;
            generation_ = generation;
        }

        public Token getStorageId()
        {
            return myToken;
        }

        public void setStorageId(Token storageId)
        {
            myToken = storageId;
        }

        public int getGeneration()
        {
            return generation_;
        }
    }

    public DBManager() throws Throwable
    {
        Set<String> tables = DatabaseDescriptor.getTableToColumnFamilyMap().keySet();
        
        for (String table : tables)
        {
            Table tbl = Table.open(table);
            tbl.onStart();
        }
        /* Do recovery if need be. */
        RecoveryManager recoveryMgr = RecoveryManager.instance();
        recoveryMgr.doRecovery();
    }

    /*
     * This method reads the system table and retrieves the metadata
     * associated with this storage instance. Currently we store the
     * metadata in a Column Family called LocatioInfo which has two
     * columns namely "Token" and "Generation". This is the token that
     * gets gossiped around and the generation info is used for FD.
    */
    public DBManager.StorageMetadata start() throws IOException
    {
        StorageMetadata storageMetadata = null;
        /* Read the system table to retrieve the storage ID and the generation */
        SystemTable sysTable = SystemTable.openSystemTable(SystemTable.name_);
        Row row = sysTable.get(FBUtilities.getHostName());

        IPartitioner p = StorageService.getPartitioner();
        if ( row == null )
        {
            Token token = p.getDefaultToken();
            int generation = 1;

            String key = FBUtilities.getHostName();
            row = new Row(key);
            ColumnFamily cf = new ColumnFamily(SystemTable.cfName_);
            cf.addColumn(new Column(SystemTable.token_, p.getTokenFactory().toByteArray(token)));
            cf.addColumn(new Column(SystemTable.generation_, BasicUtilities.intToByteArray(generation)) );
            row.addColumnFamily(cf);
            sysTable.apply(row);
            storageMetadata = new StorageMetadata( token, generation);
        }
        else
        {
            /* we crashed and came back up need to bump generation # */
        	Map<String, ColumnFamily> columnFamilies = row.getColumnFamilyMap();
        	Set<String> cfNames = columnFamilies.keySet();

            for ( String cfName : cfNames )
            {
            	ColumnFamily columnFamily = columnFamilies.get(cfName);

                IColumn tokenColumn = columnFamily.getColumn(SystemTable.token_);
                Token token = p.getTokenFactory().fromByteArray(tokenColumn.value());

                IColumn generation = columnFamily.getColumn(SystemTable.generation_);
                int gen = BasicUtilities.byteArrayToInt(generation.value()) + 1;

                Column generation2 = new Column("Generation", BasicUtilities.intToByteArray(gen), generation.timestamp() + 1);
                columnFamily.addColumn(generation2);
                storageMetadata = new StorageMetadata(token, gen);
                break;
            }
            sysTable.reset(row);
        }
        return storageMetadata;
    }

    public static void main(String[] args) throws Throwable
    {
        DBManager.instance().start();
    }
}

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
import com.facebook.infrastructure.utils.BasicUtilities;
import com.facebook.infrastructure.utils.FBUtilities;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class DBManager
{
    private static DBManager dbMgr_;
    private static Lock lock_ = new ReentrantLock();

    public static DBManager instance() throws IOException {
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
        private BigInteger storageId_;
        private int generation_;

        StorageMetadata(BigInteger storageId, int generation)
        {
            storageId_ = storageId;
            generation_ = generation;
        }

        public BigInteger getStorageId()
        {
            return storageId_;
        }

        public void setStorageId(BigInteger storageId)
        {
            storageId_ = storageId;
        }

        public int getGeneration()
        {
            return generation_;
        }
    }

    public DBManager() throws IOException {
        /* Read the configuration file */
        Map<String, Set<String>> tableToColumnFamilyMap = DatabaseDescriptor.getTableToColumnFamilyMap();
        Set<String> tables = tableToColumnFamilyMap.keySet();
        
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

        if ( row == null )
        {
            /* Generate a token for this Storage node */
            Random random = new Random();
            BigInteger token = new BigInteger(160, random);
            if ( token.signum() == -1 )
                token = token.multiply(BigInteger.valueOf(-1L));

            int generation = 1;

            String key = FBUtilities.getHostName();
            row = new Row(key);
            ColumnFamily cf = new ColumnFamily(SystemTable.cfName_);
            cf.addColumn(new Column(SystemTable.token_, token.toByteArray()) );
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

                IColumn token = columnFamily.getColumn(SystemTable.token_);
                BigInteger bi = new BigInteger( token.value() );

                IColumn generation = columnFamily.getColumn(SystemTable.generation_);
                int gen = BasicUtilities.byteArrayToInt(generation.value()) + 1;

                Column generation2 = new Column("Generation", BasicUtilities.intToByteArray(gen), generation.timestamp() + 1);
                columnFamily.addColumn(generation2);
                storageMetadata = new StorageMetadata( bi, gen );
                break;
            }
            sysTable.reset(row);
        }
        return storageMetadata;
    }
}

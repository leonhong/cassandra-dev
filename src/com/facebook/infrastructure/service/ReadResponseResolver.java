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

package com.facebook.infrastructure.service;

import com.facebook.infrastructure.db.ColumnFamily;
import com.facebook.infrastructure.db.ReadResponse;
import com.facebook.infrastructure.db.Row;
import com.facebook.infrastructure.db.RowMutation;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.util.*;

/*
 * This class is used by all read functions and is called by the Qorum 
 * when atleast a few of the servers ( few is specified in Quorum)
 * have sent the response . The resolve fn then schedules read repair 
 * and resolution of read data from the various servers.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class ReadResponseResolver implements IResponseResolver<Row>
{

	private static Logger logger_ = Logger.getLogger(WriteResponseResolver.class);

	/*
	 * This method for resolving read data should look at the timestamps of each
	 * of the columns that are read and should pick up columns with the latest
	 * timestamp. For those columns where the timestamp is not the latest a
	 * repair request should be scheduled.
	 * 
	 */
	public Row resolve(List<Message<byte[]>> responses) throws DigestMismatchException
	{
        long startTime = System.currentTimeMillis();
		Row retRow = null;
		List<Row> rowList = new ArrayList<Row>();
		List<EndPoint> endPoints = new ArrayList<EndPoint>();
		String key = null;
		String table = null;
		byte[] digest = ArrayUtils.EMPTY_BYTE_ARRAY;
		boolean isDigestQuery = false;
        
        /*
		 * Populate the list of rows from each of the messages
		 * Check to see if there is a digest query. If a digest 
         * query exists then we need to compare the digest with 
         * the digest of the data that is received.
        */
        DataInputBuffer bufIn = new DataInputBuffer();
		for (Message<byte[]> response : responses)
		{					            
            byte[] body = response.getMessageBody();
            bufIn.reset(body, body.length);
            long start = System.currentTimeMillis();
            ReadResponse result = null;
            try {
                result = ReadResponse.serializer().deserialize(bufIn);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            logger_.trace( "Response deserialization time : " + (System.currentTimeMillis() - start) + " ms.");
            if(!result.isDigestQuery())
            {
                rowList.add(result.row());
                endPoints.add(response.getFrom());
                key = result.row().key();
                table = result.table();
            }
            else
            {
                digest = result.digest();
                isDigestQuery = true;
            }
		}
		// If there was a digest query compare it withh all teh data digests 
		// If there is a mismatch then thwrow an exception so that read repair can happen.
		if(isDigestQuery)
		{
			for(Row row: rowList)
			{
				if( !Arrays.equals(row.digest(), digest) )
				{
					throw new DigestMismatchException("The Digest does not match");
				}
			}
		}
		
        /* If the rowList is empty then we had some exception above. */
        if ( rowList.size() == 0 )
        {
            return retRow;
        }
        
        /* Now calculate the resolved row */
		retRow = new Row(key);		
		for (int i = 0 ; i < rowList.size(); i++)
		{
			retRow.repair(rowList.get(i));			
		}
        // At  this point  we have the return row .
		// Now we need to calculate the differnce 
		// so that we can schedule read repairs 
		
		for (int i = 0 ; i < rowList.size(); i++)
		{
			// calculate the difference , since retRow is the resolved
			// row it can be used as the super set , remember no deletes 
			// will happen with diff its only for additions so far 
			// TODO : handle deletes 
			Row diffRow = rowList.get(i).diff(retRow);
			if(diffRow == null) // no repair needs to happen
				continue;
			// create the row mutation message based on the diff and schedule a read repair 
			RowMutation rowMutation = new RowMutation(table, key);
	        for ( ColumnFamily cf : diffRow.getColumnFamilies() )
	        {
	            rowMutation.add(cf);
	        }
	        // schedule the read repair
	        ReadRepairManager.instance().schedule(endPoints.get(i),rowMutation);
		}
        logger_.trace("resolve: " + (System.currentTimeMillis() - startTime) + " ms.");
		return retRow;
	}

	public boolean isDataPresent(List<Message<byte[]>> responses)
	{
		boolean isDataPresent = false;
		for (Message<byte[]> response : responses)
		{
            byte[] body = response.getMessageBody();
			DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            try
            {
    			ReadResponse result = ReadResponse.serializer().deserialize(bufIn);
    			if(!result.isDigestQuery())
    			{
    				isDataPresent = true;
    			}
                bufIn.close();
            }
            catch(IOException ex)
            {
                logger_.info(LogUtil.throwableToString(ex));
            }                        
		}
		return isDataPresent;
	}
}

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

import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.utils.LogUtil;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ReadRepairVerbHandler implements IVerbHandler<byte[]>
{
    private static Logger logger_ = Logger.getLogger(ReadRepairVerbHandler.class);    
    
    public void doVerb(Message<byte[]> message)
    {          
        byte[] body = message.getMessageBody();
        DataInputBuffer buffer = new DataInputBuffer();
        buffer.reset(body, body.length);        
        
        try
        {
            RowMutation rm = RowMutation.serializer().deserialize(buffer);
            rm.apply();                                   
        }
        catch( ColumnFamilyNotDefinedException ex )
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
        catch ( IOException e )
        {
            logger_.debug(LogUtil.throwableToString(e));            
        }        
    }
}

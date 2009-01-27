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
package com.facebook.infrastructure.test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.facebook.infrastructure.io.SequenceFile;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.utils.LogUtil;

public class TestRunner
{
    private static EndPoint to_ = new EndPoint("tdsearch001.sf2p.facebook.com", 7000);
    
    public static void main(String[] args) throws Throwable
    {
        LogUtil.init();
        MessagingService.getMessagingInstance().registerVerbHandlers("TEST", new TestVerbHandler());
        MessagingService.getMessagingInstance().listen(to_, false);
    }
}

class TestVerbHandler implements IVerbHandler
{
    private static EndPoint to_ = new EndPoint("tdsearch001.sf2p.facebook.com", 7000);
    
    public void doVerb(Message message)
    {
        Object[] body = message.getMessageBody();
        byte[] bytes = (byte[])body[0];
        System.out.println( new String(bytes) );
        
        Message response = message.getReply(to_, body);
        MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());
    }
}

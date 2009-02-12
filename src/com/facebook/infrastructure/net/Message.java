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

package com.facebook.infrastructure.net;

import java.lang.reflect.Array;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import com.facebook.infrastructure.io.ICompactSerializer;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Message
{    
    private static final MessageSerializer serializer_ = new MessageSerializer();

    public static MessageSerializer serializer()
    {
        return serializer_;
    }
    
    final Header header;
    private final byte[] body;

    protected Message(String id, EndPoint from, String messageType, String verb, byte[] body)
    {
        this(new Header(id, from, messageType, verb), body);
    }
    
    protected Message(Header header, byte[] body)
    {
        assert body != null;
        this.header = header;
        this.body = body;
    }

    public Message(EndPoint from, String messageType, String verb, byte[] body)
    {
        this(new Header(from, messageType, verb), body);
    }    
    
    public byte[] getHeader(Object key)
    {
        return header.getDetail(key);
    }
    
    public void removeHeader(Object key)
    {
        header.removeDetail(key);
    }
    
    public void setMessageType(String type)
    {
        header.setMessageType(type);
    }

    public void setMessageVerb(String verb)
    {
        header.setMessageVerb(verb);
    }

    public void addHeader(String key, byte[] value)
    {
        header.addDetail(key, value);
    }
    
    public Map<String, byte[]> getHeaders()
    {
        return header.getDetails();
    }

    public byte[] getMessageBody()
    {
        return body;
    }

    public EndPoint getFrom()
    {
        return header.getFrom();
    }

    public String getMessageType()
    {
        return header.getMessageType();
    }

    public String getVerb()
    {
        return header.getVerb();
    }

    public String getMessageId()
    {
        return header.getMessageId();
    }

    void setMessageId(String id)
    {
        header.setMessageId(id);
    }    

    public Message getReply(EndPoint from, byte[] body)
    {        
        return new Message(getMessageId(),
                           from,
                           MessagingService.responseStage_,
                           MessagingService.responseVerbHandler_,
                           body);
    }
    
    public String toString()
    {
        StringBuffer sbuf = new StringBuffer("");
        String separator = System.getProperty("line.separator");
        sbuf.append("ID:" + getMessageId());
        sbuf.append(separator);
        sbuf.append("FROM:" + getFrom());
        sbuf.append(separator);
        sbuf.append("TYPE:" + getMessageType());
        sbuf.append(separator);
        sbuf.append("VERB:" + getVerb());
        return sbuf.toString();
    }

    public static class MessageSerializer implements ICompactSerializer<Message>
    {
        public void serialize(Message t, DataOutputStream dos) throws IOException
        {
            Header.serializer().serialize( t.header, dos);
            byte[] bytes = t.getMessageBody();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }

        public Message deserialize(DataInputStream dis) throws IOException
        {
            Header header = Header.serializer().deserialize(dis);
            int size = dis.readInt();
            byte[] bytes = new byte[size];
            dis.readFully(bytes);
            return new Message(header, bytes);
        }
    }
}

package com.facebook.infrastructure.net;

import com.facebook.infrastructure.io.ICompactSerializer;

import java.util.Map;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;

public class Message<T> {
    private static final MessageSerializer serializer_ = new MessageSerializer();

    public static MessageSerializer serializer()
    {
        return serializer_;
    }

    final Header header;
    private final T body;

    /**
     * Use this constructor when creating replies!  You must give the same id as the original message!
     */
    public Message(String id, EndPoint from, String messageType, String verb, T body)
    {
        this(new Header(id, from, messageType, verb), body);
    }

    public Message(EndPoint from, String messageType, String verb, T body)
    {
        this(new Header(from, messageType, verb), body);
    }

    protected Message(Header header, T body)
    {
        assert body != null;
        this.header = header;
        this.body = body;
    }

    public byte[] getHeader(Object key)
    {
        return header.getDetail(key);
    }

    public void removeHeader(Object key)
    {
        header.removeDetail(key);
    }

    public void addHeader(String key, byte[] value)
    {
        header.addDetail(key, value);
    }

    public Map<String, byte[]> getHeaders()
    {
        return header.getDetails();
    }

    public T getMessageBody()
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

    public Message<T> getReply(EndPoint from, T body)
    {
        return new Message<T>(getMessageId(),
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

    public static class MessageSerializer implements ICompactSerializer<Message<byte[]>>
    {
        public void serialize(Message<byte[]> t, DataOutputStream dos) throws IOException
        {
            Header.serializer().serialize( t.header, dos);
            byte[] bytes = t.getMessageBody();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }

        public Message<byte[]> deserialize(DataInputStream dis) throws IOException
        {
            Header header = Header.serializer().deserialize(dis);
            int size = dis.readInt();
            byte[] bytes = new byte[size];
            dis.readFully(bytes);
            return new Message<byte[]>(header, bytes);
        }
    }
}

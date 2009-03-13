package org.apache.cassandra.dht;

public class StringToken extends Token<String>
{
    protected StringToken(String token)
    {
        super(token);
    }

    public int compareTo(Token<String> o)
    {
        return token.compareTo(o.token);
    }
}

package com.facebook.infrastructure.locator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.net.EndPoint;

/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the 3 nodes that lie right next to each other
 * on the ring.
 */
public class RackUnawareStrategy extends AbstractStrategy
{ 
    public RackUnawareStrategy(TokenMetadata tokenMetadata)
    {
        super(tokenMetadata);
    }
    
    public EndPoint[] getStorageEndPoints(BigInteger token)
    {
        return getStorageEndPoints(token, tokenMetadata_.cloneTokenEndPointMap());            
    }
    
    public EndPoint[] getStorageEndPoints(BigInteger token, Map<BigInteger, EndPoint> tokenToEndPointMap)
    {
        List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
        Collections.sort(tokens); // TODO can we do this only once per token update?
        int index = Collections.binarySearch(tokens, token);
        // find "primary" endpoint
        final int tokensSize = tokens.size();
        if(index < 0)
        {
            index = (index + 1) * (-1);
            if (index >= tokensSize)
                index = 0;
        }

        // how many tokens should we return?
        int N = DatabaseDescriptor.getReplicationFactor();
        if (N > tokensSize) {
            logger_.warn("Replication factor is " + N + " but only " + tokensSize + " nodes available");
            N = tokensSize;
        }

        // loop through the list, starting with the "primary" endpoint that binarysearch found, and add until we have N nodes.
        List<EndPoint> endPoints = new ArrayList<EndPoint>();
        for (int i = index; endPoints.size() < N; i = (i + 1) % tokensSize)
        {
            endPoints.add(tokenToEndPointMap.get(tokens.get(i)));
        }
        retrofitPorts(endPoints);

        return endPoints.toArray(new EndPoint[endPoints.size()]);
    }
}

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

package com.facebook.infrastructure.dht;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

import com.facebook.infrastructure.locator.TokenMetadata;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.service.StorageService;
import com.facebook.infrastructure.utils.LogUtil;

/**
 * This class handles the boostrapping responsibilities for
 * any new endpoint.
*/
public class BootStrapper implements Runnable
{
    private static Logger logger_ = Logger.getLogger(BootStrapper.class);
    /* endpoints that need to be bootstrapped */
    protected EndPoint[] targets_ = new EndPoint[0];
    /* tokens of the nodes being bootstapped. */
    protected BigInteger[] tokens_ = new BigInteger[0];
    protected TokenMetadata tokenMetadata_ = null;
    private List<EndPoint> filters_ = new ArrayList<EndPoint>();

    public BootStrapper(EndPoint[] target, BigInteger[] token)
    {
        targets_ = target;
        tokens_ = token;
        tokenMetadata_ = StorageService.instance().getTokenMetadata();
    }
    
    public BootStrapper(EndPoint[] target, BigInteger[] token, EndPoint[] filters)
    {
        this(target, token);
        Collections.addAll(filters_, filters);
    }

    public void run()
    {
        try
        {
            logger_.debug("Beginning bootstrap process for " + targets_ + " ...");                                                               
            /* copy the token to endpoint map */
            Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
            /* remove the tokens associated with the endpoints being bootstrapped */                
            for ( BigInteger token : tokens_ )
            {
                tokenToEndPointMap.remove(token);                    
            }

            Set<BigInteger> oldTokens = new HashSet<BigInteger>( tokenToEndPointMap.keySet() );
            Range[] oldRanges = StorageService.instance().getAllRanges(oldTokens);
            logger_.debug("Total number of old ranges " + oldRanges.length);
            /* 
             * Find the ranges that are split. Maintain a mapping between
             * the range being split and the list of subranges.
            */                
            Map<Range, List<Range>> splitRanges = LeaveJoinProtocolHelper.getRangeSplitRangeMapping(oldRanges, tokens_);                                                      
            /* Calculate the list of nodes that handle the old ranges */
            Map<Range, List<EndPoint>> oldRangeToEndPointMap = StorageService.instance().constructRangeToEndPointMap(oldRanges, tokenToEndPointMap);
            /* Mapping of split ranges to the list of endpoints responsible for the range */                
            Map<Range, List<EndPoint>> replicasForSplitRanges = new HashMap<Range, List<EndPoint>>();                                
            Set<Range> rangesSplit = splitRanges.keySet();                
            for ( Range splitRange : rangesSplit )
            {
                replicasForSplitRanges.put( splitRange, oldRangeToEndPointMap.get(splitRange) );
            }                
            /* Remove the ranges that are split. */
            for ( Range splitRange : rangesSplit )
            {
                oldRangeToEndPointMap.remove(splitRange);
            }
            
            /* Add the subranges of the split range to the map with the same replica set. */
            for ( Range splitRange : rangesSplit )
            {
                List<Range> subRanges = splitRanges.get(splitRange);
                List<EndPoint> replicas = replicasForSplitRanges.get(splitRange);
                for ( Range subRange : subRanges )
                {
                    /* Make sure we clone or else we are hammered. */
                    oldRangeToEndPointMap.put(subRange, new ArrayList<EndPoint>(replicas));
                }
            }                
            
            /* Add the new token and re-calculate the range assignments */
            Collections.addAll( oldTokens, tokens_ );
            Range[] newRanges = StorageService.instance().getAllRanges(oldTokens);

            logger_.debug("Total number of new ranges " + newRanges.length);
            /* Calculate the list of nodes that handle the new ranges */
            Map<Range, List<EndPoint>> newRangeToEndPointMap = StorageService.instance().constructRangeToEndPointMap(newRanges);
            /* Calculate ranges that need to be sent and from whom to where */
            Map<Range, List<BootstrapSourceTarget>> rangesWithSourceTarget = LeaveJoinProtocolHelper.getRangeSourceTargetInfo(oldRangeToEndPointMap, newRangeToEndPointMap);
            /* Send messages to respective folks to stream data over to the new nodes being bootstrapped */
            LeaveJoinProtocolHelper.assignWork(rangesWithSourceTarget, filters_);                
        }
        catch ( Throwable th )
        {
            logger_.debug( LogUtil.throwableToString(th) );
        }
    }
 
    private Range getMyOldRange()
    {
        Map<EndPoint, BigInteger> oldEndPointToTokenMap = tokenMetadata_.cloneEndPointTokenMap();
        Map<BigInteger, EndPoint> oldTokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();

        oldEndPointToTokenMap.remove(targets_);
        oldTokenToEndPointMap.remove(tokens_);

        BigInteger myToken = oldEndPointToTokenMap.get(StorageService.getLocalStorageEndPoint());
        List<BigInteger> allTokens = new ArrayList<BigInteger>(oldTokenToEndPointMap.keySet());
        Collections.sort(allTokens);
        int index = Collections.binarySearch(allTokens, myToken);
        /* Calculate the lhs for the range */
        BigInteger lhs = (index == 0) ? allTokens.get(allTokens.size() - 1) : allTokens.get( index - 1);
        return new Range( lhs, myToken );
    }
}

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

package org.apache.cassandra.service;

import java.math.BigInteger;
import java.util.Comparator;
import java.text.Collator;

public class OrderPreservingHashPartitioner implements IPartitioner
{
    private final static int maxKeyHashLength_ = 24;
    private static final BigInteger prime_ = BigInteger.valueOf(31);
    private static final Comparator<String> comparator = new Comparator<String>() {
        public int compare(String o1, String o2)
        {
            return o2.compareTo(o1);
        }
    };


    public BigInteger hash(String key)
    {
        BigInteger h = BigInteger.ZERO;
        char val[] = key.toCharArray();
        
        for (int i = 0; i < OrderPreservingHashPartitioner.maxKeyHashLength_; i++)
        {
            if( i < val.length )
                h = OrderPreservingHashPartitioner.prime_.multiply(h).add( BigInteger.valueOf(val[i]) );
            else
                h = OrderPreservingHashPartitioner.prime_.multiply(h).add( OrderPreservingHashPartitioner.prime_ );
        }
        return h;
    }

    public String decorateKey(String key)
    {
        return key;
    }

    public String undecorateKey(String decoratedKey)
    {
        return decoratedKey;
    }

    public Comparator<String> getReverseDecoratedKeyComparator()
    {
        return comparator;
    }
}

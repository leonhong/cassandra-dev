/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.apache.cassandra.test;

import java.util.Random;

import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.service.*;

/**
 *
 * @author kranganathan
 */
public class testStorage
{
    public static void main(String[] args)
    {
    	try
    	{
	        LogUtil.init();
	        StorageService s = StorageService.instance();
	        s.start();

	        testMultipleKeys.testCompactions();
    	}
    	catch (Throwable t)
    	{
    		t.printStackTrace();
    	}
    }
}

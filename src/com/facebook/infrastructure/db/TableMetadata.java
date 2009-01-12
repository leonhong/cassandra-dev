package com.facebook.infrastructure.db;

import com.facebook.infrastructure.io.*;
import com.facebook.infrastructure.config.DatabaseDescriptor;
import com.facebook.infrastructure.utils.BloomFilter;
import com.facebook.infrastructure.utils.LogUtil;

import java.io.IOException;
import java.io.File;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;

/*
 * This class represents the metadata of this Table. The metadata
 * is basically the column family name and the ID associated with
 * this column family. We use this ID in the Commit Log header to
 * determine when a log file that has been rolled can be deleted.
*/
class TableMetadata
{
    private static Logger logger_ = Logger.getLogger(Table.class);

    /* Name of the column family */
    public final static String cfName_ = "TableMetadata";
    /*
     * Name of one of the columns. The other columns are the individual
     * column families in the system.
    */
    public static final String cardinality_ = "PrimaryCardinality";
    private static ICompactSerializer<TableMetadata> serializer_;
    static
    {
        serializer_ = new TableMetadataSerializer();
    }

    private static TableMetadata tableMetadata_;
    /* Use the following writer/reader to write/read to Metadata table */
    private static IFileWriter writer_;
    private static IFileReader reader_;

    public static TableMetadata instance() throws IOException
    {
        if ( tableMetadata_ == null )
        {
            String file = getFileName();
            writer_ = SequenceFile.writer(file);
            reader_ = SequenceFile.reader(file);
            TableMetadata.load();
            if ( tableMetadata_ == null )
                tableMetadata_ = new TableMetadata();
        }
        return tableMetadata_;
    }

    static ICompactSerializer<TableMetadata> serializer()
    {
        return serializer_;
    }

    private static void load() throws IOException
    {
        String file = TableMetadata.getFileName();
        File f = new File(file);
        if ( f.exists() )
        {
            DataOutputBuffer bufOut = new DataOutputBuffer();
            DataInputBuffer bufIn = new DataInputBuffer();

            if ( reader_ == null )
            {
                reader_ = SequenceFile.reader(file);
            }

            while ( !reader_.isEOF() )
            {
                /* Read the metadata info. */
                reader_.next(bufOut);
                bufIn.reset(bufOut.getData(), bufOut.getLength());

                /* The key is the table name */
                String key = bufIn.readUTF();
                /* read the size of the data we ignore this value */
                bufIn.readInt();
                tableMetadata_ = TableMetadata.serializer().deserialize(bufIn);
                break;
            }
        }
    }

    /* The mapping between column family and the column type. */
    private Map<String, String> cfTypeMap_ = new HashMap<String, String>();
    private Map<String, Integer> cfIdMap_ = new HashMap<String, Integer>();
    private Map<Integer, String> idCfMap_ = new HashMap<Integer, String>();

    private static String getFileName()
    {
        String table = DatabaseDescriptor.getTables().get(0);
        return DatabaseDescriptor.getMetadataDirectory() + System.getProperty("file.separator") + table + "-Metadata.db";
    }

    public void add(String cf, int id)
    {
        add(cf, id, "Standard");
    }

    public void add(String cf, int id, String type)
    {
        cfIdMap_.put(cf, id);
        idCfMap_.put(id, cf);
        cfTypeMap_.put(cf, type);
    }

    boolean isEmpty()
    {
        return cfIdMap_.isEmpty();
    }

    int getColumnFamilyId(String columnFamily)
    {
        return cfIdMap_.get(columnFamily);
    }

    String getColumnFamilyName(int id)
    {
        return idCfMap_.get(id);
    }

    String getColumnFamilyType(String cfName)
    {
        return cfTypeMap_.get(cfName);
    }

    void setColumnFamilyType(String cfName, String type)
    {
        cfTypeMap_.put(cfName, type);
    }

    Set<String> getColumnFamilies()
    {
        return cfIdMap_.keySet();
    }

    int size()
    {
        return cfIdMap_.size();
    }

    boolean isValidColumnFamily(String cfName)
    {
        return cfIdMap_.containsKey(cfName);
    }

    BloomFilter.CountingBloomFilter cardinality()
    {
        return null;
    }

    void apply() throws IOException
    {
        String table = DatabaseDescriptor.getTables().get(0);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        TableMetadata.serializer_.serialize(this, bufOut);
        try
        {
            writer_.append(table, bufOut);
        }
        catch ( IOException ex )
        {
            writer_.seek(0L);
            logger_.debug(LogUtil.throwableToString(ex));
        }
    }

    public void reset() throws IOException
    {
        writer_.seek(0L);
        apply();
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        Set<String> cfNames = cfIdMap_.keySet();

        for ( String cfName : cfNames )
        {
            sb.append(cfName);
            sb.append("---->");
            sb.append(cfIdMap_.get(cfName));
            sb.append(System.getProperty("line.separator"));
        }

        return sb.toString();
    }


    static class TableMetadataSerializer implements ICompactSerializer<TableMetadata>
    {
        public void serialize(TableMetadata tmetadata, DataOutputStream dos) throws IOException
        {
            int size = tmetadata.cfIdMap_.size();
            dos.writeInt(size);
            Set<String> cfNames = tmetadata.cfIdMap_.keySet();

            for ( String cfName : cfNames )
            {
                dos.writeUTF(cfName);
                dos.writeInt( tmetadata.cfIdMap_.get(cfName).intValue() );
                dos.writeUTF(tmetadata.getColumnFamilyType(cfName));
            }
        }

        public TableMetadata deserialize(DataInputStream dis) throws IOException
        {
            TableMetadata tmetadata = new TableMetadata();
            int size = dis.readInt();
            for( int i = 0; i < size; ++i )
            {
                String cfName = dis.readUTF();
                int id = dis.readInt();
                String type = dis.readUTF();
                tmetadata.add(cfName, id, type);
            }
            return tmetadata;
        }
    }
}

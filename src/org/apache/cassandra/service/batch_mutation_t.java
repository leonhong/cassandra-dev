/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.cassandra.service;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.facebook.thrift.*;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class batch_mutation_t implements TBase, java.io.Serializable {
  public String table;
  public String key;
  public Map<String,List<column_t>> cfmap;
  public Map<String,List<column_t>> cfmapdel;

  public final Isset __isset = new Isset();
  public static final class Isset implements java.io.Serializable {
    public boolean table = false;
    public boolean key = false;
    public boolean cfmap = false;
    public boolean cfmapdel = false;
  }

  public batch_mutation_t() {
  }

  public batch_mutation_t(
    String table,
    String key,
    Map<String,List<column_t>> cfmap,
    Map<String,List<column_t>> cfmapdel)
  {
    this();
    this.table = table;
    this.__isset.table = true;
    this.key = key;
    this.__isset.key = true;
    this.cfmap = cfmap;
    this.__isset.cfmap = true;
    this.cfmapdel = cfmapdel;
    this.__isset.cfmapdel = true;
  }

  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof batch_mutation_t)
      return this.equals((batch_mutation_t)that);
    return false;
  }

  public boolean equals(batch_mutation_t that) {
    if (that == null)
      return false;

    boolean this_present_table = true && (this.table != null);
    boolean that_present_table = true && (that.table != null);
    if (this_present_table || that_present_table) {
      if (!(this_present_table && that_present_table))
        return false;
      if (!this.table.equals(that.table))
        return false;
    }

    boolean this_present_key = true && (this.key != null);
    boolean that_present_key = true && (that.key != null);
    if (this_present_key || that_present_key) {
      if (!(this_present_key && that_present_key))
        return false;
      if (!this.key.equals(that.key))
        return false;
    }

    boolean this_present_cfmap = true && (this.cfmap != null);
    boolean that_present_cfmap = true && (that.cfmap != null);
    if (this_present_cfmap || that_present_cfmap) {
      if (!(this_present_cfmap && that_present_cfmap))
        return false;
      if (!this.cfmap.equals(that.cfmap))
        return false;
    }

    boolean this_present_cfmapdel = true && (this.cfmapdel != null);
    boolean that_present_cfmapdel = true && (that.cfmapdel != null);
    if (this_present_cfmapdel || that_present_cfmapdel) {
      if (!(this_present_cfmapdel && that_present_cfmapdel))
        return false;
      if (!this.cfmapdel.equals(that.cfmapdel))
        return false;
    }

    return true;
  }

  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case 1:
          if (field.type == TType.STRING) {
            this.table = iprot.readString();
            this.__isset.table = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2:
          if (field.type == TType.STRING) {
            this.key = iprot.readString();
            this.__isset.key = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3:
          if (field.type == TType.MAP) {
            {
              TMap _map0 = iprot.readMapBegin();
              this.cfmap = new HashMap<String,List<column_t>>(2*_map0.size);
              for (int _i1 = 0; _i1 < _map0.size; ++_i1)
              {
                String _key2;
                List<column_t> _val3;
                _key2 = iprot.readString();
                {
                  TList _list4 = iprot.readListBegin();
                  _val3 = new ArrayList<column_t>(_list4.size);
                  for (int _i5 = 0; _i5 < _list4.size; ++_i5)
                  {
                    column_t _elem6 = new column_t();
                    _elem6 = new column_t();
                    _elem6.read(iprot);
                    _val3.add(_elem6);
                  }
                  iprot.readListEnd();
                }
                this.cfmap.put(_key2, _val3);
              }
              iprot.readMapEnd();
            }
            this.__isset.cfmap = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 4:
          if (field.type == TType.MAP) {
            {
              TMap _map7 = iprot.readMapBegin();
              this.cfmapdel = new HashMap<String,List<column_t>>(2*_map7.size);
              for (int _i8 = 0; _i8 < _map7.size; ++_i8)
              {
                String _key9;
                List<column_t> _val10;
                _key9 = iprot.readString();
                {
                  TList _list11 = iprot.readListBegin();
                  _val10 = new ArrayList<column_t>(_list11.size);
                  for (int _i12 = 0; _i12 < _list11.size; ++_i12)
                  {
                    column_t _elem13 = new column_t();
                    _elem13 = new column_t();
                    _elem13.read(iprot);
                    _val10.add(_elem13);
                  }
                  iprot.readListEnd();
                }
                this.cfmapdel.put(_key9, _val10);
              }
              iprot.readMapEnd();
            }
            this.__isset.cfmapdel = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  public void write(TProtocol oprot) throws TException {
    TStruct struct = new TStruct("batch_mutation_t");
    oprot.writeStructBegin(struct);
    TField field = new TField();
    if (this.table != null) {
      field.name = "table";
      field.type = TType.STRING;
      field.id = 1;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.table);
      oprot.writeFieldEnd();
    }
    if (this.key != null) {
      field.name = "key";
      field.type = TType.STRING;
      field.id = 2;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.key);
      oprot.writeFieldEnd();
    }
    if (this.cfmap != null) {
      field.name = "cfmap";
      field.type = TType.MAP;
      field.id = 3;
      oprot.writeFieldBegin(field);
      {
        oprot.writeMapBegin(new TMap(TType.STRING, TType.LIST, this.cfmap.size()));
        for (String _iter14 : this.cfmap.keySet())        {
          oprot.writeString(_iter14);
          {
            oprot.writeListBegin(new TList(TType.STRUCT, this.cfmap.get(_iter14).size()));
            for (column_t _iter15 : this.cfmap.get(_iter14))            {
              _iter15.write(oprot);
            }
            oprot.writeListEnd();
          }
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.cfmapdel != null) {
      field.name = "cfmapdel";
      field.type = TType.MAP;
      field.id = 4;
      oprot.writeFieldBegin(field);
      {
        oprot.writeMapBegin(new TMap(TType.STRING, TType.LIST, this.cfmapdel.size()));
        for (String _iter16 : this.cfmapdel.keySet())        {
          oprot.writeString(_iter16);
          {
            oprot.writeListBegin(new TList(TType.STRUCT, this.cfmapdel.get(_iter16).size()));
            for (column_t _iter17 : this.cfmapdel.get(_iter16))            {
              _iter17.write(oprot);
            }
            oprot.writeListEnd();
          }
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("batch_mutation_t(");
    sb.append("table:");
    sb.append(this.table);
    sb.append(",key:");
    sb.append(this.key);
    sb.append(",cfmap:");
    sb.append(this.cfmap);
    sb.append(",cfmapdel:");
    sb.append(this.cfmapdel);
    sb.append(")");
    return sb.toString();
  }

}


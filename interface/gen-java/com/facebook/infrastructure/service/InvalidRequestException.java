/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.facebook.infrastructure.service;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.facebook.thrift.*;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class InvalidRequestException extends Exception implements TBase, java.io.Serializable {
  public String why;
  public static final int WHY = 1;

  public final Isset __isset = new Isset();
  public static final class Isset implements java.io.Serializable {
    public boolean why = false;
  }

  public InvalidRequestException() {
  }

  public InvalidRequestException(
    String why)
  {
    this();
    this.why = why;
    this.__isset.why = (why != null);
  }

  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof InvalidRequestException)
      return this.equals((InvalidRequestException)that);
    return false;
  }

  public boolean equals(InvalidRequestException that) {
    if (that == null)
      return false;

    boolean this_present_why = true && (this.why != null);
    boolean that_present_why = true && (that.why != null);
    if (this_present_why || that_present_why) {
      if (!(this_present_why && that_present_why))
        return false;
      if (!this.why.equals(that.why))
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
        case WHY:
          if (field.type == TType.STRING) {
            this.why = iprot.readString();
            this.__isset.why = true;
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
    TStruct struct = new TStruct("InvalidRequestException");
    oprot.writeStructBegin(struct);
    TField field = new TField();
    if (this.why != null) {
      field.name = "why";
      field.type = TType.STRING;
      field.id = WHY;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.why);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("InvalidRequestException(");
    boolean first = true;

    if (!first) sb.append(", ");
    sb.append("why:");
    sb.append(this.why);
    first = false;
    sb.append(")");
    return sb.toString();
  }

}

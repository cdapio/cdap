package com.continuuity.data2.transaction;

import com.continuuity.data2.transaction.distributed.thrift.TTransaction;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.IOException;

/**
 * Handles serialization and deserialization of {@link Transaction} instances to and from {@code byte[]}.
 */
public class TransactionCodec {
  public static final String TX_OPERATION_ATTRIBUTE_KEY = "continuuity.tx";

  public TransactionCodec() {
  }

  public byte[] encode(Transaction tx) throws IOException {
    TTransaction thriftTx = new TTransaction(tx.getWritePointer(), tx.getReadPointer(),
                                             Longs.asList(tx.getInvalids()), Longs.asList(tx.getInProgress()),
                                             tx.getFirstShortInProgress());
    TSerializer serializer = new TSerializer();
    try {
      return serializer.serialize(thriftTx);
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  public Transaction decode(byte[] encoded) throws IOException {
    TTransaction thriftTx = new TTransaction();
    TDeserializer deserializer = new TDeserializer();
    try {
      deserializer.deserialize(thriftTx, encoded);
      return new Transaction(thriftTx.getReadPointer(), thriftTx.getWritePointer(),
                             Longs.toArray(thriftTx.getInvalids()), Longs.toArray(thriftTx.getInProgress()),
                             thriftTx.getFirstShort());
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  public void addToOperation(OperationWithAttributes op, Transaction tx) throws IOException {
    op.setAttribute(TX_OPERATION_ATTRIBUTE_KEY, encode(tx));
  }

  public Transaction getFromOperation(OperationWithAttributes op) throws IOException {
    byte[] encoded = op.getAttribute(TX_OPERATION_ATTRIBUTE_KEY);
    if (encoded != null) {
      return decode(encoded);
    }
    return null;
  }
}

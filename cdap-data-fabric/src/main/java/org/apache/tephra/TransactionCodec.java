/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra;

import org.apache.tephra.distributed.TransactionConverterUtils;
import org.apache.tephra.distributed.thrift.TTransaction;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.IOException;

/**
 * Handles serialization and deserialization of {@link Transaction} instances to and from {@code byte[]}.
 */
public class TransactionCodec {

  public TransactionCodec() {
  }

  public byte[] encode(Transaction tx) throws IOException {
    TTransaction thriftTx = TransactionConverterUtils.wrap(tx);
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
      return TransactionConverterUtils.unwrap(thriftTx);
    } catch (TException te) {
      throw new IOException(te);
    }
  }
}

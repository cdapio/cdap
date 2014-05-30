package com.continuuity.hive.context;

import com.continuuity.common.io.Codec;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import java.io.IOException;

/**
 * Codec to encode/decode a Transaction.
 */
public class TxnCodec implements Codec<Transaction> {
  private static final Gson GSON = new Gson();

  public static final TxnCodec INSTANCE = new TxnCodec();

  private TxnCodec() {
    // Use the static INSTANCE to get an instance.
  }

  @Override
  public byte[] encode(Transaction object) throws IOException {
    return GSON.toJson(object).getBytes(Charsets.UTF_8);
  }

  @Override
  public Transaction decode(byte[] data) throws IOException {
    Preconditions.checkNotNull(data, "Transaction ID is empty.");
    return GSON.fromJson(new String(data, Charsets.UTF_8), Transaction.class);
  }
}

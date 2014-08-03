/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.hive.context;

import co.cask.cdap.common.io.Codec;
import com.continuuity.tephra.Transaction;
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

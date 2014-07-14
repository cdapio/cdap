/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.persist;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.google.common.collect.Sets;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.IOException;

/**
 * test for {@link TransactionEdit}
 */
public class TransactionEditTest {
  @Test
  public void testV1SerdeCompat() throws Exception {
    // start tx edit and committed tx edit cover all fields of tx edit
    // NOTE: set visibilityUpperBound to 0 as this is expected default for decoding older version that doesn't store it
    verifyDecodingSupportsV1(TransactionEdit.createStarted(2L, 0L, 1000L, 3L));
    verifyDecodingSupportsV1(TransactionEdit.createCommitted(2L,
                                                                      Sets.newHashSet(new ChangeId(Bytes.toBytes("c"))),
                                                                      3L, true));
  }

  @SuppressWarnings("deprecation")
  private void verifyDecodingSupportsV1(TransactionEdit edit) throws IOException {
    TransactionEdit.TransactionEditCodecV1 v1Codec = new TransactionEdit.TransactionEditCodecV1();
    // encoding with codec of v1
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    v1Codec.encode(edit, out);

    // decoding
    TransactionEdit decodedEdit = new TransactionEdit();
    DataInput in = ByteStreams.newDataInput(out.toByteArray());
    decodedEdit.readFields(in);

    Assert.assertEquals(edit, decodedEdit);
  }
}

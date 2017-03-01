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

package org.apache.tephra.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tephra.ChangeId;
import org.apache.tephra.TransactionType;
import org.apache.tephra.persist.TransactionEdit;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Util class for {@link TransactionEdit} related tests.
 */
public final class TransactionEditUtil {
  private static Random random = new Random();

  /**
   * Generates a number of semi-random {@link TransactionEdit} instances.
   */
  public static List<TransactionEdit> createRandomEdits(int numEntries) {
    List<co.cask.tephra.persist.TransactionEdit> caskTxEdits = createRandomCaskEdits(numEntries);
    List<TransactionEdit> txEdits = new ArrayList<>();
    for (co.cask.tephra.persist.TransactionEdit caskTxEdit : caskTxEdits) {
      txEdits.add(TransactionEdit.convertCaskTxEdit(caskTxEdit));
    }
    return txEdits;
  }

  /**
   * Generates a number of semi-random {@link co.cask.tephra.persist.TransactionEdit} instances.
   * These are just randomly selected from the possible states, so would not necessarily reflect a real-world
   * distribution.
   *
   * @param numEntries how many entries to generate in the returned list.
   * @return a list of randomly generated transaction log edits.
   */
  @Deprecated
  public static List<co.cask.tephra.persist.TransactionEdit> createRandomCaskEdits(int numEntries) {
    List<co.cask.tephra.persist.TransactionEdit> edits = Lists.newArrayListWithCapacity(numEntries);
    for (int i = 0; i < numEntries; i++) {
      co.cask.tephra.persist.TransactionEdit.State nextType =
        co.cask.tephra.persist.TransactionEdit.State.values()[random.nextInt(6)];
      long writePointer = Math.abs(random.nextLong());
      switch (nextType) {
        case INPROGRESS:
          edits.add(
            co.cask.tephra.persist.TransactionEdit.createStarted(writePointer, writePointer - 1,
                                                                 System.currentTimeMillis() + 300000L,
                                                                 TransactionType.SHORT));
          break;
        case COMMITTING:
          edits.add(co.cask.tephra.persist.TransactionEdit.createCommitting(writePointer, generateChangeSet(10)));
          break;
        case COMMITTED:
          edits.add(co.cask.tephra.persist.TransactionEdit.createCommitted(writePointer, generateChangeSet(10),
                                                                           writePointer + 1, random.nextBoolean()));
          break;
        case INVALID:
          edits.add(co.cask.tephra.persist.TransactionEdit.createInvalid(writePointer));
          break;
        case ABORTED:
          edits.add(co.cask.tephra.persist.TransactionEdit.createAborted(writePointer, TransactionType.SHORT, null));
          break;
        case MOVE_WATERMARK:
          edits.add(co.cask.tephra.persist.TransactionEdit.createMoveWatermark(writePointer));
          break;
      }
    }
    return edits;
  }

  private static Set<ChangeId> generateChangeSet(int numEntries) {
    Set<ChangeId> changes = Sets.newHashSet();
    for (int i = 0; i < numEntries; i++) {
      byte[] bytes = new byte[8];
      random.nextBytes(bytes);
      changes.add(new ChangeId(bytes));
    }
    return changes;
  }
}

/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.data;

import javax.annotation.Nullable;

/**
 * An interface representing a message to be published.
 */
public interface PublishableMessage {

  /**
   * Returns the payload of the message. If {@code null} is returned, it requires {@link #isTransactional()} to return
   * {@code true} and {@link #getTransactionWritePointer()} should return the transaction write pointer for
   * messages stored in the Payload Table prior to this call.
   */
  @Nullable
  byte[] getPayload();

  /**
   * Returns {@code true} if the message should be published transactionally.
   */
  boolean isTransactional();

  /**
   * Returns the transaction write pointer if the message is going to be published transactionally, that is
   * when {@link #isTransactional()} returns {@code true}.
   */
  long getTransactionWritePointer();
}

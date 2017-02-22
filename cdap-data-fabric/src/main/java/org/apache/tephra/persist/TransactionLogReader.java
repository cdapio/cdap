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

package org.apache.tephra.persist;

import java.io.Closeable;
import java.io.IOException;

/**
 * Represents a reader for {@link TransactionLog} instances.
 */
public interface TransactionLogReader extends Closeable {
  /**
   * Returns the next {@code TransactionEdit} from the log file, based on the current position, or {@code null}
   * if the end of the file has been reached.
   */
  TransactionEdit next() throws IOException;

  /**
   * Populates {@code reuse} with the next {@code TransactionEdit}, based on the reader's current position in the
   * log file.
   * @param reuse The {@code TransactionEdit} instance to populate with the log entry data.
   * @return The {@code TransactionEdit} instance, or {@code null} if the end of the file has been reached.
   * @throws IOException If an error is encountered reading the log data.
   */
  TransactionEdit next(TransactionEdit reuse) throws IOException;
}

/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.app.queue;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.queue.QueueName;

/**
 * This interface defines the specification for associated with either
 * input or output binds to it's respective {@link QueueName} and {@link Schema}
 */
public interface QueueSpecification {

  /**
   * @return {@link QueueName} associated with the queue.
   */
  QueueName getQueueName();

  /**
   * @return {@link Schema} associated with the reader schema of the queue.
   */
  Schema getInputSchema();

  /**
   * @return {@link Schema} associated with data written to the queue.
   */
  Schema getOutputSchema();
}

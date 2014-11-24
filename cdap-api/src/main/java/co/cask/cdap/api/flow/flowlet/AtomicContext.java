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

package co.cask.cdap.api.flow.flowlet;

/**
 * Represents the context of an atomic change made passed to a {@link Flowlet}.
 */
public interface AtomicContext {

  /**
   * Type of atomic change.
   */
  public enum Type {
    /**
     * This context is an input context, passed to the {@link Flowlet} for processing.
     */
    PROCESS_DATA,

    /**
     * An instance change context, passed to the {@link Flowlet} when the number of
     * instances changes.
     */
    INSTANCE_CHANGE
  }

  /**
   * @return Type of atomic change that this {@link AtomicContext} represents.
   */
  Type getType();

  /**
   * @return Name of the output the event was read from.
   */
  String getOrigin();

  /**
   * @return Number of attempts made to process the event, if {@link FailurePolicy} is set to RETRY else zero.
   */
  int getRetryCount();
}

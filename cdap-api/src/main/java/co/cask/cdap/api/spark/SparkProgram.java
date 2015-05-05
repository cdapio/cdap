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

package co.cask.cdap.api.spark;

import java.io.Serializable;

/**
 * Defines an interface for User's Spark job. User should implements one of the sub-interfaces instead of this one,
 * base on the language choice.
 *
 * This interface extends serializable because the closures are anonymous class in Java and Spark Serializes the
 * closures before sending it to worker nodes. This serialization of inner anonymous class expects the outer
 * containing class to be serializable else java.io.NotSerializableException is thrown. We do not expect user job
 * class to be serializable so we serialize this interface which user job class implements to have a neater API.
 */
public interface SparkProgram extends Serializable {

  /**
   * User Spark job which will be executed
   *
   * @param context {@link SparkContext} for this job
   */
  public void run(SparkContext context);
}

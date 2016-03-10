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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;

/**
 * An abstract base class for implementing a {@link HttpServiceHandler} that runs inside a Spark program.
 * It provides access to {@link org.apache.spark.SparkContext SparkContext} through the {@link #getSparkContext()}
 * method.
 */
public abstract class AbstractSparkHttpServiceHandler extends AbstractHttpServiceHandler {

  private org.apache.spark.SparkContext sparkContext;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);

    // Shouldn't happen. The CDAP framework guarantees it.
    if (!(context instanceof SparkHttpServiceContext)) {
      throw new IllegalArgumentException("The context type should be SparkHttpServiceContext");
    }
    this.sparkContext = ((SparkHttpServiceContext) context).getSparkContext();
  }

  /**
   * @return The {@link org.apache.spark.SparkContext} of the running Spark program
   */
  protected final org.apache.spark.SparkContext getSparkContext() {
    return sparkContext;
  }
}

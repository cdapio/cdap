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

package co.cask.cdap.app.runtime.spark.stream;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContextProvider;
import co.cask.cdap.data.stream.AbstractStreamInputFormat;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * StreamInputFomat for {@link Spark} jobs.
 *
 * @param <K> Key type of input
 * @param <V> Value type of input
 */
public class SparkStreamInputFormat<K, V> extends AbstractStreamInputFormat<K, V> {

  @Override
  public AuthorizationEnforcer getAuthorizationEnforcer(TaskAttemptContext context) {
    return SparkRuntimeContextProvider.get().getAuthorizationEnforcer();
  }

  @Override
  public AuthenticationContext getAuthenticationContext(TaskAttemptContext context) {
    return SparkRuntimeContextProvider.get().getAuthenticationContext();
  }
}

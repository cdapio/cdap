/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.internal.app.runtime.DefaultTaskLocalizationContext;

import java.io.Externalizable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * An {@link Externalizable} implementation of {@link TaskLocalizationContext} that can be used by Spark programs
 * to retrieve localized resources in Spark Executors. It uses {@link SparkContextProvider} to
 * get the {@link ExecutionSparkContext} in the current execution context.
 */
public class SparkLocalizationContext implements TaskLocalizationContext, Externalizable {

  private final TaskLocalizationContext delegate;

  public SparkLocalizationContext() {
    this.delegate = SparkContextProvider.getSparkContext().getTaskLocalizationContext();
  }

  public SparkLocalizationContext(Map<String, File> localizedResources) {
    this.delegate = new DefaultTaskLocalizationContext(localizedResources);
  }

  @Override
  public File getLocalFile(String name) throws FileNotFoundException {
    return delegate.getLocalFile(name);
  }

  @Override
  public Map<String, File> getAllLocalFiles() {
    return delegate.getAllLocalFiles();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    //no-op
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    //no-op
  }
}

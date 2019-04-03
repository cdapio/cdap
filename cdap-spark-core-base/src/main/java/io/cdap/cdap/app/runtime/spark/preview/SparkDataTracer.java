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
package co.cask.cdap.app.runtime.spark.preview;

import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContext;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContextProvider;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A {@link Externalizable} implementation of {@link DataTracer} used in Spark program execution.
  * It delegates all operations to the {@link DataTracer} constructed using {@link SparkRuntimeContext}.
  */
public class SparkDataTracer implements DataTracer, Externalizable {

  private transient DataTracer dataTracer;

  public SparkDataTracer() {
    // no-op for default constructor
  }

  /**
   * Constructor. It delegates {@link DataTracer} operations to {@link DataTracer} constructed
   * by {@link SparkRuntimeContext}
   */
  public SparkDataTracer(SparkRuntimeContext sparkRuntimeContext, String tracerName) {
    this.dataTracer = sparkRuntimeContext.getDataTracer(tracerName);
  }

  @Override
  public void info(String propertyName, Object propertyValue) {
    dataTracer.info(propertyName, propertyValue);
  }

  @Override
  public String getName() {
    return dataTracer.getName();
  }

  @Override
  public boolean isEnabled() {
    return dataTracer.isEnabled();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(dataTracer.getName());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    String tracerName = in.readUTF();
    this.dataTracer = SparkRuntimeContextProvider.get().getDataTracer(tracerName);
  }
}

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

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.app.services.AbstractServiceDiscoverer;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.ProgramId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A {@link Externalizable} implementation of {@link ServiceDiscoverer} used in Spark program execution.
 * It has no-op for serialize/deserialize operations. It uses {@link SparkContextProvider} to
 * get the {@link ExecutionSparkContext} in the current execution context and
 * uses the {@link DiscoveryServiceClient} from the context object.
 */
public class SparkServiceDiscoverer extends AbstractServiceDiscoverer implements Externalizable {

  private final DiscoveryServiceClient discoveryServiceClient;

  public SparkServiceDiscoverer() {
    this(SparkContextProvider.getSparkContext());
  }

  public SparkServiceDiscoverer(ExecutionSparkContext context) {
    this(context.getProgramId().toEntityId(), context.getDiscoveryServiceClient());
  }

  public SparkServiceDiscoverer(ProgramId programId, DiscoveryServiceClient discoveryServiceClient) {
    super(programId);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  protected DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // no-op
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // no-op
  }
}

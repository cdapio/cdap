/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.app.services;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.app.program.Program;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * A {@link Serializable} {@link ServiceDiscoverer}. This is needed for {@link Spark} program which expects
 * object used in the Closure to be {@link Serializable}
 */
public class SerializableServiceDiscoverer extends AbstractServiceDiscoverer implements Externalizable {

  private static final long serialVersionUID = 6547316362453719580L;
  private static DiscoveryServiceClient discoveryServiceClient;

  // no-arg constructor required for serialization/deserialization to work
  public SerializableServiceDiscoverer() {
  }

  public static void setDiscoveryServiceClient(DiscoveryServiceClient discoveryServiceClient) {
    SerializableServiceDiscoverer.discoveryServiceClient = discoveryServiceClient;
  }

  public SerializableServiceDiscoverer(Program program) {
    super(program);
  }

  @Override
  public void writeExternal(ObjectOutput objectOutput) throws IOException {
    objectOutput.writeObject(namespaceId);
    objectOutput.writeObject(applicationId);
  }

  @Override
  public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
    namespaceId = (String) objectInput.readObject();
    applicationId = (String) objectInput.readObject();
  }

  @Override
  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }
}

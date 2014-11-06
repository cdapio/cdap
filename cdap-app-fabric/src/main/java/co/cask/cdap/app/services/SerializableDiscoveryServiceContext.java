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

package co.cask.cdap.app.services;

import co.cask.cdap.api.DiscoveryServiceContext;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.app.program.Program;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A {@link Serializable} {@link DiscoveryServiceContext}. This is needed for {@link Spark} program which expects
 * object used in the Closure to be {@link Serializable}
 */
public class SerializableDiscoveryServiceContext extends AbstractDiscoveryServiceContext implements Serializable {

  private static final long serialVersionUID = 6547316362453719580L;
  private static DiscoveryServiceClient discoveryServiceClient;

  // no-arg constructor required for serialization/deserialization to work
  public SerializableDiscoveryServiceContext() {
  }

  public SerializableDiscoveryServiceContext(Program program, DiscoveryServiceClient discoveryClient) {
    super(program);
    discoveryServiceClient = discoveryClient;
  }

  // custom serialization
  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.defaultWriteObject();
    oos.writeObject(accountId);
    oos.writeObject(applicationId);
  }

  // custom deserialization
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    ois.defaultReadObject();
    accountId = (String) ois.readObject();
    applicationId = (String) ois.readObject();
  }

  @Override
  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }
}

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

package co.cask.cdap.test.remote;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.test.ProcedureClient;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class RemoteProcedureClient implements ProcedureClient {

  private final co.cask.cdap.client.ProcedureClient procedureClient;
  private final String applicationId;
  private final String procedureName;

  public RemoteProcedureClient(ClientConfig clientConfig, String applicationId, String procedureName) {
    this.procedureClient = new co.cask.cdap.client.ProcedureClient(clientConfig);
    this.applicationId = applicationId;
    this.procedureName = procedureName;
  }

  @Override
  public byte[] queryRaw(String method, Map<String, String> arguments) throws IOException {
    try {
      return Bytes.toBytes(procedureClient.call(applicationId, procedureName, method, arguments));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String query(String method, Map<String, String> arguments) throws IOException {
    try {
      return procedureClient.call(applicationId, procedureName, method, arguments);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

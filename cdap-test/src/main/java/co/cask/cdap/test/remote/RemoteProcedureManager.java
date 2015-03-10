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

package co.cask.cdap.test.remote;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.ProcedureClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ProcedureManager;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class RemoteProcedureManager implements ProcedureManager {

  private final Id.Procedure procedure;
  private final ClientConfig clientConfig;
  private final MetricsClient metricsClient;

  public RemoteProcedureManager(Id.Procedure procedure, ClientConfig clientConfig) {
    this.procedure = procedure;
    this.clientConfig = clientConfig;
    this.metricsClient = new MetricsClient(clientConfig);
  }

  private ClientConfig getClientConfig() {
    return new ClientConfig.Builder(clientConfig).setNamespace(procedure.getNamespace()).build();
  }

  private ProgramClient getProgramClient() {
    return new ProgramClient(getClientConfig());
  }

  @Override
  public void stop() {
    try {
      getProgramClient().stop(procedure.getApplicationId(), procedure.getType(), procedure.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public RuntimeMetrics getMetrics() {
    return metricsClient.getProcedureMetrics(procedure);
  }

  @Override
  public co.cask.cdap.test.ProcedureClient getClient() {
    return new ProcedureClientAdapter(clientConfig, procedure);
  }

  /**
   * Transforms cdap-client ProcedureClient into cdap-test ProcedureClient
   */
  public static class ProcedureClientAdapter implements co.cask.cdap.test.ProcedureClient {

    private final ClientConfig clientConfig;
    private final Id.Procedure procedure;

    public ProcedureClientAdapter(ClientConfig clientConfig, Id.Procedure procedure) {
      this.clientConfig = clientConfig;
      this.procedure = procedure;
    }

    private ClientConfig getClientConfig() {
      return new ClientConfig.Builder(clientConfig).setNamespace(procedure.getNamespace()).build();
    }

    private ProcedureClient getProcedureClient() {
      return new ProcedureClient(getClientConfig());
    }

    @Override
    public byte[] queryRaw(String method, Map<String, String> arguments) throws IOException {
      try {
        return getProcedureClient().callRaw(procedure.getApplicationId(), procedure.getId(), method, arguments);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public String query(String method, Map<String, String> arguments) throws IOException {
      try {
        return getProcedureClient().call(procedure.getApplicationId(), procedure.getId(), method, arguments);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}

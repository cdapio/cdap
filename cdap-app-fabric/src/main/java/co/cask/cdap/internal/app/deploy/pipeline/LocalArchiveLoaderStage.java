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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.ForwardingApplicationSpecification;
import co.cask.cdap.internal.app.deploy.InMemoryConfigurator;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.filesystem.Location;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * LocalArchiveLoaderStage gets a {@link Location} and emits a {@link ApplicationDeployable}.
 * <p>
 * This stage is responsible for reading the JAR and generating an ApplicationSpecification
 * that is forwarded to the next stage of processing.
 * </p>
 */
public class LocalArchiveLoaderStage extends AbstractStage<Location> {
  private final CConfiguration cConf;
  private final ApplicationSpecificationAdapter adapter;
  private final Id.Namespace id;
  private final String appId;

  /**
   * Constructor with hit for handling type.
   */
  public LocalArchiveLoaderStage(CConfiguration cConf, Id.Namespace id, @Nullable String appId) {
    super(TypeToken.of(Location.class));
    this.cConf = cConf;
    this.id = id;
    this.appId = appId;
    this.adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
  }

  /**
   * Creates a {@link co.cask.cdap.internal.app.deploy.InMemoryConfigurator} to run through
   * the process of generation of {@link ApplicationSpecification}
   *
   * @param archive Location of archive.
   */
  @Override
  public void process(Location archive) throws Exception {
    InMemoryConfigurator inMemoryConfigurator = new InMemoryConfigurator(id, archive);
    ListenableFuture<ConfigResponse> result = inMemoryConfigurator.config();
    //TODO: Check with Terence on how to handle this stuff.
    ConfigResponse response = result.get(120, TimeUnit.SECONDS);
    ApplicationSpecification specification = adapter.fromJson(response.get());
    if (appId != null) {
      specification = new ForwardingApplicationSpecification(specification) {
        @Override
        public String getName() {
          return appId;
        }
      };
    }

    emit(new ApplicationDeployable(cConf, Id.Application.from(id, specification.getName()), specification, archive));
  }
}

/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.worker;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * ConfiguratorTask is a RunnableTask for performing the configurator config.
 */
public class ConfiguratorTask implements RunnableTask {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
    .registerTypeAdapter(Requirements.class, new RequirementsCodec())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(ConfiguratorTask.class);

  private final CConfiguration cConf;

  @Inject
  ConfiguratorTask(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    AppDeploymentInfo deploymentInfo = GSON.fromJson(context.getParam(), AppDeploymentInfo.class);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocalLocationModule(),
      new ConfiguratorTaskModule()
    );
    ConfigResponse result = injector.getInstance(ConfiguratorTaskRunner.class).configure(deploymentInfo);
    context.writeResult(GSON.toJson(result).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Class to preform the configurator task execution.
   */
  private static class ConfiguratorTaskRunner {
    private final Impersonator impersonator;
    private final PluginFinder pluginFinder;
    private final ArtifactRepository artifactRepository;
    private final CConfiguration cConf;
    private final ArtifactLocalizerClient artifactLocalizerClient;

    @Inject
    ConfiguratorTaskRunner(Impersonator impersonator, PluginFinder pluginFinder,
                                  ArtifactRepository artifactRepository, CConfiguration cConf,
                                  ArtifactLocalizerClient artifactLocalizerClient) {
      this.impersonator = impersonator;
      this.pluginFinder = pluginFinder;
      this.artifactRepository = artifactRepository;
      this.cConf = cConf;
      this.artifactLocalizerClient = artifactLocalizerClient;
    }

    public ConfigResponse configure(AppDeploymentInfo info) throws Exception {
      // Getting the pipeline app from appfabric
      LOG.debug("Fetching artifact '{}' from app-fabric to create artifact class loader.", info.getArtifactId());

      Location artifactLocation = artifactLocalizerClient.getArtifactLocation(info.getArtifactId());

      try (InputStream is = artifactRepository.newInputStream(Id.Artifact.fromEntityId(info.getArtifactId()));
           OutputStream os = artifactLocation.getOutputStream()) {
        ByteStreams.copy(is, os);
      }

      LOG.debug("Successfully fetched artifact '{}'.", info.getArtifactId());

      // Creates a new deployment info with the newly fetched artifact
      AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(info, artifactLocation);
      InMemoryConfigurator configurator = new InMemoryConfigurator(cConf, pluginFinder, impersonator,
                                                                   artifactRepository, deploymentInfo);
      try {
        return configurator.config().get(120, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        // We don't need the ExecutionException being reported back to the RemoteTaskExecutor, hence only
        // propagating the actual cause.
        Throwables.propagateIfPossible(e.getCause(), Exception.class);
        throw Throwables.propagate(e.getCause());
      }
    }
  }
}

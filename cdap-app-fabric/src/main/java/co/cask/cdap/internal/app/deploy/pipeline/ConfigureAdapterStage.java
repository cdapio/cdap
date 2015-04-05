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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.InMemoryAdapterConfigurator;
import co.cask.cdap.internal.app.deploy.InMemoryConfigurator;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterSpecification;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * LocalArchiveLoaderStage gets a {@link Location} and emits a {@link ApplicationDeployable}.
 * <p>
 * This stage is responsible for reading the JAR and generating an ApplicationSpecification
 * that is forwarded to the next stage of processing.
 * </p>
 */
public class ConfigureAdapterStage extends AbstractStage<AdapterDeploymentInfo> {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Logger LOG = LoggerFactory.getLogger(ConfigureAdapterStage.class);
  private final Id.Namespace namespace;
  private final String adapterName;

  /**
   * Constructor with hit for handling type.
   */
  public ConfigureAdapterStage(Id.Namespace namespace, String adapterName) {
    super(TypeToken.of(AdapterDeploymentInfo.class));
    this.namespace = namespace;
    this.adapterName = adapterName;
  }

  /**
   * Creates a {@link InMemoryConfigurator} to run through
   * the process of generation of {@link ApplicationSpecification}
   *
   * @param deploymentInfo Location of the input and output location
   */
  @Override
  public void process(AdapterDeploymentInfo deploymentInfo) throws Exception {

    Location outputLocation = deploymentInfo.getTempJarLoc();
    Location parent = Locations.getParent(outputLocation);
    Locations.mkdirsIfNotExists(parent);

    File input = deploymentInfo.getTemplateInfo().getFile();
    Location tmpLocation = parent.getTempFile(".tmp");
    LOG.debug("Copy from {} to {}", input.getName(), tmpLocation.toURI());
    Files.copy(input, Locations.newOutputSupplier(tmpLocation));

    // Finally, move archive to final location
    try {
      if (tmpLocation.renameTo(outputLocation) == null) {
        throw new IOException(String.format("Could not move archive from location: %s, to location: %s",
                                            tmpLocation.toURI(), outputLocation.toURI()));
      }
    } catch (IOException e) {
      // In case copy to temporary file failed, or rename failed
      tmpLocation.delete();
      throw e;
    }

    InMemoryAdapterConfigurator inMemoryAdapterConfigurator =
      new InMemoryAdapterConfigurator(namespace, new LocalLocationFactory().create(input.toURI()), adapterName,
                                      deploymentInfo.getAdapterConfig(), deploymentInfo.getTemplateSpec());
    AdapterSpecification spec = GSON.fromJson(inMemoryAdapterConfigurator.config().get().get(),
                                              AdapterSpecification.class);
    emit(spec);
  }
}

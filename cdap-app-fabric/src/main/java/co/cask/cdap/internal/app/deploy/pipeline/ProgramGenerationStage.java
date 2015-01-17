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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.webapp.WebappSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.archive.ArchiveBundler;
import co.cask.cdap.common.conf.Configuration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.app.program.ProgramBundle;
import co.cask.cdap.internal.app.runtime.webapp.WebappProgramRunner;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 *
 */
public class ProgramGenerationStage extends AbstractStage<ApplicationDeployable> {
  private final LocationFactory locationFactory;
  private final Configuration configuration;

  public ProgramGenerationStage(Configuration configuration, LocationFactory locationFactory) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.configuration = configuration;
    this.locationFactory = locationFactory;
  }

  @Override
  public void process(final ApplicationDeployable input) throws Exception {
    ImmutableList.Builder<Program> programs = ImmutableList.builder();
    final ApplicationSpecification appSpec = input.getSpecification();
    final String applicationName = appSpec.getName();

    final ArchiveBundler bundler = new ArchiveBundler(input.getLocation());

    // Make sure we have a directory to store the original artifact.
    Location outputDir = locationFactory.create(configuration.get(Constants.AppFabric.OUTPUT_DIR));
    final Location newOutputDir = outputDir.append(input.getId().getNamespaceId());

    // Check exists, create, check exists again to avoid failure due to race condition.
    if (!newOutputDir.exists() && !newOutputDir.mkdirs() && !newOutputDir.exists()) {
      throw new IOException("Failed to create directory");
    }

    // Now, we iterate through all ProgramSpecification and generate programs
    Iterable<ProgramSpecification> specifications = Iterables.concat(
      appSpec.getMapReduce().values(),
      appSpec.getFlows().values(),
      appSpec.getProcedures().values(),
      appSpec.getWorkflows().values(),
      appSpec.getServices().values(),
      appSpec.getSpark().values()
    );

    // Generate webapp program if required
    Set<String> servingHostNames = WebappProgramRunner.getServingHostNames(
      Locations.newInputSupplier(input.getLocation()));

    if (!servingHostNames.isEmpty()) {
      specifications = Iterables.concat(specifications, ImmutableList.of(
        createWebappSpec(ProgramType.WEBAPP.toString().toLowerCase())));
    }

    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
      Executors.newFixedThreadPool(10, Threads.createDaemonThreadFactory("program-gen-%d"))
    );
    try {
      List<ListenableFuture<Location>> futures = Lists.newArrayList();
      for (final ProgramSpecification spec: specifications) {
        ListenableFuture<Location> future = executorService.submit(
          new Callable<Location>() {
          @Override
          public Location call() throws Exception {
            ProgramType type = ProgramTypes.fromSpecification(spec);
            String name = String.format(Locale.ENGLISH, "%s/%s", type, applicationName);
            Location programDir = newOutputDir.append(name);
            if (!programDir.exists()) {
              programDir.mkdirs();
            }
            Location output = programDir.append(String.format("%s.jar", spec.getName()));
            return ProgramBundle.create(input.getId(), bundler, output, spec.getName(),
                                        spec.getClassName(), type, appSpec);
            }
        });
        futures.add(future);
      }

      for (Location jarLocation : Futures.allAsList(futures).get()) {
        programs.add(Programs.create(jarLocation, null));
      }
    } finally {
      executorService.shutdown();
    }

    // Emits the received specification with programs.
    emit(new ApplicationWithPrograms(input, programs.build()));
  }

  private WebappSpecification createWebappSpec(final String name) {
    return new WebappSpecification() {
      @Override
      public String getClassName() {
        return "";
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getDescription() {
        return "";
      }
    };
  }
}

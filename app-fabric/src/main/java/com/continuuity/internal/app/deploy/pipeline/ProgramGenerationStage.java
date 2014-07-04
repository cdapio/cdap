package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.common.conf.Configuration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.program.ProgramBundle;
import com.continuuity.internal.app.runtime.webapp.WebappProgramRunner;
import com.continuuity.internal.app.runtime.webapp.WebappSpecification;
import com.continuuity.pipeline.AbstractStage;
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
public class ProgramGenerationStage extends AbstractStage<ApplicationSpecLocation> {
  private final LocationFactory locationFactory;
  private final Configuration configuration;

  public ProgramGenerationStage(Configuration configuration, LocationFactory locationFactory) {
    super(TypeToken.of(ApplicationSpecLocation.class));
    this.configuration = configuration;
    this.locationFactory = locationFactory;
  }

  @Override
  public void process(final ApplicationSpecLocation o) throws Exception {
    ImmutableList.Builder<Program> programs = ImmutableList.builder();
    final ApplicationSpecification appSpec = o.getSpecification();
    final String applicationName = appSpec.getName();

    final ArchiveBundler bundler = new ArchiveBundler(o.getArchive());

    // Make sure we have a directory to store the original artifact.
    Location outputDir = locationFactory.create(configuration.get(Constants.AppFabric.OUTPUT_DIR));
    final Location newOutputDir = outputDir.append(o.getApplicationId().getAccountId());

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
      appSpec.getServices().values()
    );

    // Generate webapp program if required
    Set<String> servingHostNames = WebappProgramRunner.getServingHostNames(o.getArchive().getInputStream());
    if (!servingHostNames.isEmpty()) {
      specifications = Iterables.concat(specifications,
                                        ImmutableList.of(createWebappSpec(Type.WEBAPP.toString().toLowerCase())));
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
            Type type = Type.typeOfSpecification(spec);
            String name = String.format(Locale.ENGLISH, "%s/%s", type, applicationName);
            Location programDir = newOutputDir.append(name);
            if (!programDir.exists()) {
              programDir.mkdirs();
            }
            Location output = programDir.append(String.format("%s.jar", spec.getName()));
            return ProgramBundle.create(o.getApplicationId(), bundler, output, spec.getName(),
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
    emit(new ApplicationWithPrograms(o, programs.build()));
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

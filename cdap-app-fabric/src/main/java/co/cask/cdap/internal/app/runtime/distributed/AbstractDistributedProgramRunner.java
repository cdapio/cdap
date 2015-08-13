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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.twill.AbortOnTimeoutEventHandler;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.security.HBaseTokenUtils;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Defines the base framework for starting {@link Program} in the cluster.
 */
public abstract class AbstractDistributedProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDistributedProgramRunner.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();

  private final TwillRunner twillRunner;
  protected final Configuration hConf;
  protected final CConfiguration cConf;
  protected final EventHandler eventHandler;

  /**
   * An interface for launching TwillApplication. Used by sub-classes only.
   */
  protected abstract class ApplicationLauncher {

    /**
     * Starts the given application through Twill.
     *
     * @param twillApplication the application to start
     *
     * @return the {@link TwillController} for the application.
     */
    public TwillController launch(TwillApplication twillApplication) {
      return launch(twillApplication, ImmutableList.<String>of());
    }

    /**
     * Starts the given application through Twill with extra classpaths appended to the end of the classpath of
     * the runnables inside the applications.
     *
     * @param twillApplication the application to start
     * @param extraClassPaths to append
     *
     * @return the {@link TwillController} for the application.
     * @see TwillPreparer#withClassPaths(Iterable)
     */
    public TwillController launch(TwillApplication twillApplication, String...extraClassPaths) {
      return launch(twillApplication, Arrays.asList(extraClassPaths));
    }

    /**
     * Starts the given application through Twill with extra classpaths appended to the end of the classpath of
     * the runnables inside the applications.
     *
     * @param twillApplication the application to start
     * @param extraClassPaths to append
     *
     * @return the {@link TwillController} for the application.
     * @see TwillPreparer#withClassPaths(Iterable)
     */
    public abstract TwillController launch(TwillApplication twillApplication, Iterable<String> extraClassPaths);
  }

  protected AbstractDistributedProgramRunner(TwillRunner twillRunner, Configuration hConf, CConfiguration cConf) {
    this.twillRunner = twillRunner;
    this.hConf = hConf;
    this.cConf = cConf;
    this.eventHandler = createEventHandler(cConf);
  }

  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE));
  }

  @Override
  public final ProgramController run(final Program program, final ProgramOptions options) {
    final String schedulerQueueName = options.getArguments().getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE);
    final File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                         cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    try {
      if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
        hConf.set(JobContext.QUEUE_NAME, schedulerQueueName);
        LOG.info("Setting scheduler queue to {}", schedulerQueueName);
      }

      Map<String, LocalizeResource> localizeResources = addAdapterPluginFiles(options,
                                                                              new HashMap<String, LocalizeResource>());

      // Copy config files and program jar to local temp, and ask Twill to localize it to container.
      // What Twill does is to save those files in HDFS and keep using them during the lifetime of application.
      // Twill will manage the cleanup of those files in HDFS.
      localizeResources.put("hConf.xml",
                            new LocalizeResource(saveHConf(hConf, File.createTempFile("hConf", ".xml", tempDir))));
      localizeResources.put("cConf.xml",
                            new LocalizeResource(saveCConf(cConf, File.createTempFile("cConf", ".xml", tempDir))));
      File programDir = DirUtils.createTempDir(tempDir);
      final Program copiedProgram = copyProgramJar(program, tempDir, programDir);

      final URI logbackURI = getLogBackURI(copiedProgram, programDir, tempDir);
      final String programOptions = GSON.toJson(options);

      // Obtains and add the HBase delegation token as well (if in non-secure mode, it's a no-op)
      // Twill would also ignore it if it is not running in secure mode.
      // The HDFS token should already obtained by Twill.
      return launch(copiedProgram, options, localizeResources, new ApplicationLauncher() {
        @Override
        public TwillController launch(TwillApplication twillApplication, Iterable<String> extraClassPaths) {
          TwillPreparer twillPreparer = twillRunner.prepare(twillApplication);
          if (options.isDebug()) {
            LOG.info("Starting {} with debugging enabled, programOptions: {}, and logback: {}",
                     program.getId(), programOptions, logbackURI);
            twillPreparer.enableDebugging();
          }
          // Add scheduler queue name if defined
          if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
            LOG.info("Setting scheduler queue for app {} as {}", program.getId(), schedulerQueueName);
            twillPreparer.setSchedulerQueue(schedulerQueueName);
          }
          if (logbackURI != null) {
            twillPreparer.withResources(logbackURI);
          }

          String yarnAppClassPath = hConf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                           Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
          TwillController twillController = twillPreparer
            .withDependencies(HBaseTableUtilFactory.getHBaseTableUtilClass())
            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
            .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())))
            .withClassPaths(Iterables.concat(extraClassPaths, Splitter.on(',').trimResults()
                              .split(hConf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH, ""))))
            .withApplicationClassPaths(Splitter.on(",").trimResults().split(yarnAppClassPath))
            .withBundlerClassAcceptor(new HadoopClassExcluder())
            .withApplicationArguments(
              String.format("--%s", RunnableOptions.JAR), copiedProgram.getJarLocation().getName(),
              String.format("--%s", RunnableOptions.PROGRAM_OPTIONS), programOptions
            ).start();
          return addCleanupListener(twillController, program, tempDir);
        }
      });
    } catch (IOException e) {
      deleteDirectory(tempDir);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Gets plugin files that needs to be localized for the adapter. If the given run is not an adapter, no
   * modification will be done to the map.
   */
  private Map<String, LocalizeResource> addAdapterPluginFiles(ProgramOptions options,
                                                              Map<String, LocalizeResource> localizeResources) {
    Arguments arguments = options.getArguments();
    if (!arguments.hasOption(ProgramOptionConstants.ADAPTER_SPEC)) {
      return localizeResources;
    }

    // Decode the adapter spec from program system argument
    AdapterDefinition adapterSpec = GSON.fromJson(arguments.getOption(ProgramOptionConstants.ADAPTER_SPEC),
                                                     AdapterDefinition.class);

    // Get all unique PluginInfo from the adapter spec
    Set<PluginInfo> plugins = adapterSpec.getPluginInfos();

    // If there is no plugin used by the adapter, nothing need to be localized
    if (plugins.isEmpty()) {
      return localizeResources;
    }

    File templateDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_DIR));
    File templatePluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR),
                                      adapterSpec.getTemplate());

    String localizePrefix = templateDir.getName() + "/" +
                            templateDir.toURI().relativize(templatePluginDir.toURI()).getPath();

    // Localize all required plugin jars and maintain the template plugin directory structure
    // The AbstractProgramTwillRunnable will set the APP_TEMPLATE_DIR correspondingly.
    for (PluginInfo plugin : plugins) {
      String localizedName = String.format("%s/%s", localizePrefix, plugin.getFileName());
      localizeResources.put(localizedName, new LocalizeResource(new File(templatePluginDir, plugin.getFileName())));
    }

    // Localize all files under template plugin "lib" directory
    for (File libJar : DirUtils.listFiles(new File(templatePluginDir, "lib"), "jar")) {
      String localizedName = String.format("%s/lib/%s", localizePrefix, libJar.getName());
      localizeResources.put(localizedName, new LocalizeResource(libJar));
    }

    return localizeResources;
  }

  /**
   * Returns a {@link URI} for the logback.xml file to be localized to container and avaiable in the container
   * classpath.
   */
  @Nullable
  private URI getLogBackURI(Program program, File programDir, File tempDir) throws IOException {
    // TODO: When CDAP-1273 is fixed you can get the resource directly from the program classloader.
    // Make an unused call to getClassloader() to ensure that the jar is expanded into programDir.
    program.getClassLoader();
    File logbackFile = new File(programDir, "logback.xml");
    if (logbackFile.exists()) {
      return logbackFile.toURI();
    }
    URL resource = getClass().getClassLoader().getResource("logback-container.xml");
    if (resource == null) {
      return null;
    }
    // Copy the template
    logbackFile = new File(tempDir, "logback.xml");
    Files.copy(Resources.newInputStreamSupplier(resource), logbackFile);
    return logbackFile.toURI();
  }

  /**
   * Sub-class overrides this method to launch the twill application.
   */
  protected abstract ProgramController launch(Program program, ProgramOptions options,
                                              Map<String, LocalizeResource> localizeResources,
                                              ApplicationLauncher launcher);


  private File saveHConf(Configuration conf, File file) throws IOException {
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  private File saveCConf(CConfiguration conf, File file) throws IOException {
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  /**
   * Copies the program jar to a local temp file and return a {@link Program} instance
   * with {@link Program#getJarLocation()} points to the local temp file.
   */
  private Program copyProgramJar(final Program program, File tempDir, File programDir) throws IOException {
    File tempJar = File.createTempFile(program.getName(), ".jar", tempDir);
    Files.copy(new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return program.getJarLocation().getInputStream();
      }
    }, tempJar);
    return Programs.createWithUnpack(Locations.toLocation(tempJar), programDir);
  }

  /**
   * Deletes the given directory recursively. Only log if there is {@link IOException}.
   */
  private void deleteDirectory(File directory) {
    try {
      DirUtils.deleteDirectoryContents(directory);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory {}", directory, e);
    }
  }

  /**
   * Adds a listener to the given TwillController to delete local temp files when the program has started/terminated.
   * The local temp files could be removed once the program is started, since Twill would keep the files in
   * HDFS and no long needs the local temp files once program is started.
   *
   * @return The same TwillController instance.
   */
  private TwillController addCleanupListener(TwillController controller,
                                             final Program program, final File tempDir) {

    final AtomicBoolean deleted = new AtomicBoolean(false);
    Runnable cleanup = new Runnable() {

      public void run() {
        if (!deleted.compareAndSet(false, true)) {
          return;
        }
        LOG.debug("Cleanup tmp files for {}: {}", program.getId(), tempDir);
        deleteDirectory(tempDir);
      }};
    controller.onRunning(cleanup, Threads.SAME_THREAD_EXECUTOR);
    controller.onTerminated(cleanup, Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }
}

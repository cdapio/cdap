/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbortOnTimeoutEventHandler;
import com.continuuity.data.security.HBaseTokenUtils;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Defines the base framework for starting {@link Program} in the cluster.
 */
public abstract class AbstractDistributedProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDistributedProgramRunner.class);

  private final TwillRunner twillRunner;
  private final Configuration hConf;
  private final CConfiguration cConf;
  protected final EventHandler eventHandler;

  /**
   * An interface for launching TwillApplication. Used by sub-classes only.
   */
  protected interface ApplicationLauncher {
    TwillController launch(TwillApplication twillApplication);
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
    final File hConfFile;
    final File cConfFile;
    final Program copiedProgram;
    final File programDir;    // Temp directory for unpacking the program

    try {
      // Copy config files and program jar to local temp, and ask Twill to localize it to container.
      // What Twill does is to save those files in HDFS and keep using them during the lifetime of application.
      // Twill will manage the cleanup of those files in HDFS.
      hConfFile = saveHConf(hConf, File.createTempFile("hConf", ".xml"));
      cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
      programDir = Files.createTempDir();
      copiedProgram = copyProgramJar(program, programDir);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    final String runtimeArgs = new Gson().toJson(options.getUserArguments());

    // Obtains and add the HBase delegation token as well (if in non-secure mode, it's a no-op)
    // Twill would also ignore it if it is not running in secure mode.
    // The HDFS token should already obtained by Twill.
    return launch(copiedProgram, options, hConfFile, cConfFile, new ApplicationLauncher() {
      @Override
      public TwillController launch(TwillApplication twillApplication) {
        TwillPreparer twillPreparer = twillRunner
          .prepare(twillApplication);
        if (options.isDebug()) {
          LOG.info("Starting {} with debugging enabled.", program.getId());
          twillPreparer.enableDebugging();
        }
        TwillController twillController = twillPreparer
          .withDependencies(new HBaseTableUtilFactory().get().getClass())
          .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
          .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())))
          .withApplicationArguments(
            String.format("--%s", RunnableOptions.JAR), copiedProgram.getJarLocation().getName(),
            String.format("--%s", RunnableOptions.RUNTIME_ARGS), runtimeArgs
          ).start();
        return addCleanupListener(twillController, hConfFile, cConfFile, copiedProgram, programDir);
      }
    });
  }

  /**
   * Sub-class overrides this method to launch the twill application.
   */
  protected abstract ProgramController launch(Program program, ProgramOptions options,
                                              File hConfFile, File cConfFile, ApplicationLauncher launcher);


  private File saveHConf(Configuration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }

  private File saveCConf(CConfiguration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }

  /**
   * Copies the program jar to a local temp file and return a {@link Program} instance
   * with {@link Program#getJarLocation()} points to the local temp file.
   */
  private Program copyProgramJar(final Program program, File programDir) throws IOException {
    File tempJar = File.createTempFile(program.getName(), ".jar");
    Files.copy(new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return program.getJarLocation().getInputStream();
      }
    }, tempJar);

    final Location jarLocation = new LocalLocationFactory().create(tempJar.toURI());
    return Programs.createWithUnpack(jarLocation, programDir);
  }

  /**
   * Adds a listener to the given TwillController to delete local temp files when the program has started/terminated.
   * The local temp files could be removed once the program is started, since Twill would keep the files in
   * HDFS and no long needs the local temp files once program is started.
   *
   * @return The same TwillController instance.
   */
  private TwillController addCleanupListener(TwillController controller, final File hConfFile,
                                             final File cConfFile, final Program program, final File programDir) {

    final AtomicBoolean deleted = new AtomicBoolean(false);
    controller.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        cleanup();
      }

      @Override
      public void terminated(Service.State from) {
        cleanup();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        cleanup();
      }

      private void cleanup() {
        if (deleted.compareAndSet(false, true)) {
          LOG.debug("Cleanup tmp files for {}: {} {} {}",
                    program.getName(), hConfFile, cConfFile, program.getJarLocation().toURI());
          hConfFile.delete();
          cConfFile.delete();
          try {
            program.getJarLocation().delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete program jar {}", program.getJarLocation().toURI(), e);
          }
          try {
            FileUtils.deleteDirectory(programDir);
          } catch (IOException e) {
            LOG.warn("Failed to delete program directory {}", programDir, e);
          }
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }
}

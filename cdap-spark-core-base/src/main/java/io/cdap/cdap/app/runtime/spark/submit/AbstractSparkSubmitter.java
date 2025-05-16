/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.submit;


import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.runtime.spark.SparkMainWrapper;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import org.apache.spark.deploy.SparkSubmit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

/**
 * Provides common implementation for different {@link SparkSubmitter}.
 */
public abstract class AbstractSparkSubmitter implements SparkSubmitter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkSubmitter.class);

  // Transforms LocalizeResource to URI string
  private static final Function<LocalizeResource, String> RESOURCE_TO_PATH = input -> input.getURI().toString();

  @Override
  public final <V> SparkJobFuture<V> submit(SparkRuntimeContext runtimeContext,
                                            Map<String, String> configs, List<LocalizeResource> resources,
                                            URI jobFile, final V result) throws Exception {
    SparkSpecification spec = runtimeContext.getSparkSpecification();

    List<String> args = createSubmitArguments(runtimeContext, configs, resources, jobFile);

    // Spark submit is called from this executor
    // Use an executor to simplify logic that is needed to interrupt the running thread on stopping
    ExecutorService executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setNameFormat("spark-submitter-" + spec.getName() + "-" + runtimeContext.getRunId())
        .build());

    // Latch for the Spark job completion
    CountDownLatch completion = new CountDownLatch(1);
    long defaultTimeoutMillis = TimeUnit.SECONDS.toMillis(
      runtimeContext.getCConfiguration().getLong(Constants.AppFabric.PROGRAM_MAX_STOP_SECONDS));

    AbstractSparkJobFuture<V> resultFuture = new AbstractSparkJobFuture<V>(defaultTimeoutMillis) {
      @Override
      protected void onCancel(long timeout, TimeUnit timeoutTimeUnit) {
        runtimeContext.setTerminationTime(System.currentTimeMillis() + timeoutTimeUnit.toMillis(timeout));

        // Try to shutdown the running spark job.
        triggerShutdown(timeout, timeoutTimeUnit);

        // Wait for the Spark-Submit returns
        Uninterruptibles.awaitUninterruptibly(completion);
      }
    };

    // Submit the Spark job
    executor.submit(() -> {
      try {
        List<String> extraArgs = beforeSubmit();
        String[] submitArgs = Iterables.toArray(Iterables.concat(args, extraArgs), String.class);
        submit(runtimeContext, submitArgs);
        boolean state = waitForFinish();
        if (!state) {
          throw new Exception("Spark driver returned error state");
        }
        onCompleted(state);
        resultFuture.complete(result);
      } catch (Throwable t) {
        onCompleted(false);
        resultFuture.completeExceptionally(t);
      } finally {
        completion.countDown();
      }
    });
    // Shutdown the executor right after submit since the thread is only used for one submission.
    executor.shutdown();
    return resultFuture;
  }

  /**
   * Add the {@code --master} argument for the Spark submission.
   * @throws Exception if there is error while getting master ip address from spark config
   */
  protected abstract void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder)
    throws Exception;

  /**
   * Invoked for stopping the Spark job explicitly.
   */
  protected abstract void triggerShutdown(long timeout, TimeUnit timeoutTimeUnit);

  /**
   * Called before submitting the Spark job.
   *
   * @return list of extra arguments to pass to {@link SparkSubmit}.
   */
  protected List<String> beforeSubmit() throws Exception {
    return Collections.emptyList();
  }

  /**
   * Called when the Spark program finished.
   *
   * @param succeeded {@code true} to indicate the program completed successfully as reported by SparkSubmit.
   */
  protected void onCompleted(boolean succeeded) {
    // no-op
  }

  /**
   * Returns configs that are specific to the submission context.
   *
   * @param appConf Spark configs specified by the application
   * @throws Exception if there is error while generating submit conf.
   */
  protected Map<String, String> generateSubmitConf(Map<String, String> appConf) throws Exception {
    return Collections.emptyMap();
  }

  /**
   * Returns iterable of archives from list of localize resources.
   */
  protected Iterable<LocalizeResource> getArchives(List<LocalizeResource> localizeResources) {
    return Iterables.filter(localizeResources, LocalizeResource::isArchive);
  }

  /**
   * Returns iterable of archives from list of localize resources.
   */
  protected Iterable<LocalizeResource> getFiles(List<LocalizeResource> localizeResources) {
    return Iterables.filter(localizeResources, Predicates.not(LocalizeResource::isArchive));
  }

  /**
   * Returns job file for spark.
   * @throws Exception if there is error getting job jar file
   */
  @Nullable
  protected URI getJobFile() throws Exception {
    return null;
  }

  /**
   * Returns true if spark driver has succeeded.
   */
  protected boolean waitForFinish() throws Exception {
    return true;
  }

  protected Function<LocalizeResource, String> getLocalizeResourceToURIFunc() {
    return RESOURCE_TO_PATH;
  }

  /**
   * Submits the Spark job using {@link SparkSubmit}.
   *
   * @param runtimeContext context representing the Spark program
   * @param args arguments for the {@link SparkSubmit#main(String[])} method.
   */
  private void submit(SparkRuntimeContext runtimeContext, String[] args) {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(runtimeContext.getProgramInvocationClassLoader());
    try {
      LOG.debug("Calling SparkSubmit for {} {}: {}",
                runtimeContext.getProgram().getId(), runtimeContext.getRunId(), Arrays.toString(args));
      // Explicitly set the SPARK_SUBMIT property as it is no longer set on the System properties by the SparkSubmit
      // after the class rewrite. This property only control logging of a warning when submitting the Spark job,
      // hence it's harmless to just leave it there.
      System.setProperty("SPARK_SUBMIT", "true");
      SparkSubmit.main(args);
      LOG.debug("SparkSubmit returned for {} {}", runtimeContext.getProgram().getId(), runtimeContext.getRunId());
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  /**
   * Creates the list of arguments that will be used for calling {@link SparkSubmit#main(String[])}.
   *
   * @param runtimeContext the {@link SparkRuntimeContext} for the spark program
   * @param configs set of Spark configurations
   * @param resources list of resources that needs to be localized to Spark containers
   * @param jobFile the job file for Spark
   * @return a list of arguments
   * @throws Exception if there is error while creating submit arguments
   */
  private List<String> createSubmitArguments(SparkRuntimeContext runtimeContext, Map<String, String> configs,
                                             List<LocalizeResource> resources, URI jobFile) throws Exception {
    SparkSpecification spec = runtimeContext.getSparkSpecification();
    LOG.warn("SANKET : createSubmitArguments : ALL LOCAL RESOURCE ");
    for (LocalizeResource lr : resources) {
      LOG.warn("SANKET : createSubmitArguments : LocalizeResource : " + lr.getURI().getPath());
    }

    LOG.warn("SANKET : createSubmitArguments : ALL CONFIGS ");
    configs.entrySet().forEach(entry -> {
      LOG.warn("SANKET : Key: " + entry.getKey() + ", Value: " + entry.getValue());
    });

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    Iterable<LocalizeResource> archivesIterable = getArchives(resources);
    Iterable<LocalizeResource> filesIterable = getFiles(resources);

    addMaster(configs, builder);
    builder.add("--conf").add("spark.app.name=" + spec.getName());

    configs.putAll(generateSubmitConf(configs));
    BiConsumer<String, String> confAdder = (k, v) -> builder.add("--conf").add(k + "=" + v);
    configs.forEach(confAdder);


    String artifactTry = null;
    LOG.warn("SANKET : createSubmitArguments : ALL archives  ");
    for (LocalizeResource lr : archivesIterable){
      LOG.warn("SANKET : archivesIterable : " + lr.getURI());
      if (lr.getURI().getPath().contains("artifacts_archive")){
        LOG.warn("SANKET : archivesIterable : COPYING : " + lr.getURI());
        File tmpDir = Files.createTempDir();
        File artifacts_archive_jar = tmpDir.toPath().resolve("artifacts_archive.jar").toFile();
        File file = new File(lr.getURI());
        Files.copy(file, artifacts_archive_jar);
        LOG.warn("SANKET : archivesIterable : COPIED to  : " + artifacts_archive_jar.getAbsolutePath());
        artifactTry = artifacts_archive_jar.getAbsolutePath();

      }
    }

    String archives = Joiner.on(',').join(Iterables.transform(archivesIterable,
                                                              getLocalizeResourceToURIFunc()));

    if (artifactTry != null){
      archives = archives + ",file:" +artifactTry;
    }

    String files = Joiner.on(',').join(Iterables.transform(filesIterable, getLocalizeResourceToURIFunc()));

    if (!Strings.isNullOrEmpty(archives)) {
      builder.add("--archives").add(archives);
    }
    if (!Strings.isNullOrEmpty(files)) {
      builder.add("--files").add(files);
    }

    String jars = "gs://0000_sanket/serverless_jars/jars/USER-datagen-plugins-0.1.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/USER-trash-plugin-1.2.0.jar,gs://0000_sanket/serverless_jars/jars/aopalliance.aopalliance-1.0.jar,gs://0000_sanket/serverless_jars/jars/cdap-etl-api-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/cdap-etl-api-spark-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/cdap-etl-batch-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/cdap-etl-core-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/cdap-etl-proto-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/ch.qos.logback.logback-classic-1.2.11.jar,gs://0000_sanket/serverless_jars/jars/ch.qos.logback.logback-core-1.2.11.jar,gs://0000_sanket/serverless_jars/jars/ch.qos.reload4j.reload4j-1.2.22.jar,gs://0000_sanket/serverless_jars/jars/com.fasterxml.jackson.core.jackson-annotations-2.15.1.jar,gs://0000_sanket/serverless_jars/jars/com.google.code.findbugs.jsr305-2.0.1.jar,gs://0000_sanket/serverless_jars/jars/com.google.code.gson.gson-2.3.1.jar,gs://0000_sanket/serverless_jars/jars/com.google.errorprone.error_prone_annotations-2.36.0.jar,gs://0000_sanket/serverless_jars/jars/com.google.guava.guava-13.0.1.jar,gs://0000_sanket/serverless_jars/jars/com.google.inject.extensions.guice-assistedinject-4.0.jar,gs://0000_sanket/serverless_jars/jars/com.google.inject.extensions.guice-multibindings-4.0.jar,gs://0000_sanket/serverless_jars/jars/com.google.inject.guice-4.0.jar,gs://0000_sanket/serverless_jars/jars/commons-beanutils.commons-beanutils-1.7.0.jar,gs://0000_sanket/serverless_jars/jars/commons-io.commons-io-2.12.0.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-api-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-api-common-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-api-spark3_2.12-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-app-fabric-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-common-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-data-fabric-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-error-api-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-features-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-formats-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-log-publisher-spi-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-master-spi-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-messaging-spi-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-metadata-spi-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-proto-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-runtime-spi-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-security-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-security-spi-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-spark-core3_2.12-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-spark-python-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-storage-spi-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-system-app-api-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-tms-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-watchdog-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.cdap.cdap-watchdog-api-6.12.0-SNAPSHOT.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.common.common-http-0.13.1.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.common.common-io-0.13.1.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.http.netty-http-1.7.0.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.twill.twill-api-1.4.0.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.twill.twill-common-1.4.0.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.twill.twill-core-1.4.0.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.twill.twill-discovery-api-1.4.0.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.twill.twill-discovery-core-1.4.0.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.twill.twill-yarn-1.4.0.jar,gs://0000_sanket/serverless_jars/jars/io.cdap.twill.twill-zookeeper-1.4.0.jar,gs://0000_sanket/serverless_jars/jars/io.dropwizard.metrics.metrics-core-3.1.2.jar,gs://0000_sanket/serverless_jars/jars/io.netty.netty-buffer-4.1.75.Final.jar,gs://0000_sanket/serverless_jars/jars/io.netty.netty-codec-4.1.75.Final.jar,gs://0000_sanket/serverless_jars/jars/io.netty.netty-codec-http-4.1.75.Final.jar,gs://0000_sanket/serverless_jars/jars/io.netty.netty-common-4.1.75.Final.jar,gs://0000_sanket/serverless_jars/jars/io.netty.netty-handler-4.1.75.Final.jar,gs://0000_sanket/serverless_jars/jars/io.netty.netty-transport-4.1.75.Final.jar,gs://0000_sanket/serverless_jars/jars/it.unimi.dsi.fastutil-6.5.6.jar,gs://0000_sanket/serverless_jars/jars/javax.inject.javax.inject-1.jar,gs://0000_sanket/serverless_jars/jars/javax.ws.rs.javax.ws.rs-api-2.0.jar,gs://0000_sanket/serverless_jars/jars/net.sf.jopt-simple.jopt-simple-3.2.jar,gs://0000_sanket/serverless_jars/jars/org.apache.avro.avro-1.11.4.jar,gs://0000_sanket/serverless_jars/jars/org.apache.commons.commons-compress-1.22.jar,gs://0000_sanket/serverless_jars/jars/org.apache.commons.commons-dbcp2-2.9.0.jar,gs://0000_sanket/serverless_jars/jars/org.apache.commons.commons-pool2-2.10.0.jar,gs://0000_sanket/serverless_jars/jars/org.apache.tephra.tephra-api-0.15.0-incubating.jar,gs://0000_sanket/serverless_jars/jars/org.apache.tephra.tephra-core-0.15.0-incubating.jar,gs://0000_sanket/serverless_jars/jars/org.apache.thrift.libthrift-0.9.3.jar,gs://0000_sanket/serverless_jars/jars/org.bouncycastle.bcpkix-jdk15on-1.70.jar,gs://0000_sanket/serverless_jars/jars/org.bouncycastle.bcprov-jdk15on-1.70.jar,gs://0000_sanket/serverless_jars/jars/org.bouncycastle.bcutil-jdk15on-1.70.jar,gs://0000_sanket/serverless_jars/jars/org.conscrypt.conscrypt-openjdk-uber-2.5.1.jar,gs://0000_sanket/serverless_jars/jars/org.fusesource.leveldbjni.leveldbjni-all-1.8.jar,gs://0000_sanket/serverless_jars/jars/org.iq80.leveldb.leveldb-0.12-uber.jar,gs://0000_sanket/serverless_jars/jars/org.ow2.asm.asm-7.1.jar,gs://0000_sanket/serverless_jars/jars/org.ow2.asm.asm-commons-7.1.jar,gs://0000_sanket/serverless_jars/jars/org.ow2.asm.asm-tree-7.1.jar,gs://0000_sanket/serverless_jars/jars/org.quartz-scheduler.quartz-2.2.0.jar,gs://0000_sanket/serverless_jars/jars/org.slf4j.jcl-over-slf4j-1.7.15.jar,gs://0000_sanket/serverless_jars/jars/org.slf4j.jul-to-slf4j-1.7.15.jar,gs://0000_sanket/serverless_jars/jars/org.slf4j.slf4j-api-1.7.15.jar,gs://0000_sanket/serverless_jars/jars/zookeeper-3.4.6.jar";
    builder.add("--jars").add(jars);

    URI newJobFile = getJobFile();
    if (newJobFile != null) {
      jobFile = newJobFile;
    }

    boolean isPySpark = jobFile.getPath().endsWith(".py");
    if (isPySpark) {
      // For python, add extra py library files
      String pyFiles = configs.get("spark.submit.pyFiles");
      if (pyFiles != null) {
        builder.add("--py-files").add(pyFiles);
      }
    } else {
      builder.add("--class").add(SparkMainWrapper.class.getName());
    }

    if ("file".equals(jobFile.getScheme())) {
      builder.add(jobFile.getPath());
    } else {
      builder.add(jobFile.toString());
    }

    if (!isPySpark) {
      // Add extra arguments for easily identifying the program from command line.
      // Arguments to user program is always coming from the runtime arguments.
      builder.add("--cdap.spark.program=" + runtimeContext.getProgramRunId().toString());
      builder.add("--cdap.user.main.class=" + spec.getMainClassName());
    }

    return builder.build();
  }
}

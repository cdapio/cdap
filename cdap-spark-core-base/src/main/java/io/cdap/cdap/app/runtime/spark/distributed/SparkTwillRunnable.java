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

package io.cdap.cdap.app.runtime.spark.distributed;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DistributedArtifactManagerModule;
import io.cdap.cdap.app.guice.UnsupportedPluginFinder;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.spark.SparkProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.distributed.AbstractProgramTwillRunnable;
import io.cdap.cdap.internal.app.spark.SparkCompatReader;
import io.cdap.cdap.master.spi.twill.Completable;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillRunnable;

/**
 * A {@link TwillRunnable} wrapper for {@link ProgramRunner} that runs spark.
 */
@Completable
public class SparkTwillRunnable extends AbstractProgramTwillRunnable<ProgramRunner> {

  SparkTwillRunnable(String name) {
    super(name);
  }

  @Override
  protected ProgramRunner createProgramRunner(Injector injector) {
    // Inside the TwillRunanble, we use the "Local" SparkRunner, since we need to actually submit the job.
    // The actual execution mode of the job is governed by the framework configuration,
    // which is in the hConf we shipped from DistributedSparkProgramRunner
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    return new SparkProgramRuntimeProvider(SparkCompatReader.get(cConf)) { }
      .createProgramRunner(ProgramType.SPARK, ProgramRuntimeProvider.Mode.LOCAL, injector);
  }

  @Override
  protected Module createModule(CConfiguration cConf, Configuration hConf,
                                ProgramOptions programOptions, ProgramRunId programRunId) {

    Module module = super.createModule(cConf, hConf, programOptions, programRunId);

    // Only supports dynamic artifacts fetching when running on-prem
    return ProgramRunners.getClusterMode(programOptions) == ClusterMode.ON_PREMISE
      ? Modules.combine(module, new DistributedArtifactManagerModule())
      : Modules.combine(module, new AbstractModule() {
      @Override
      protected void configure() {
        bind(PluginFinder.class).to(UnsupportedPluginFinder.class);
      }
    });
  }
}

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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.app.guice.DistributedArtifactManagerModule;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.app.runtime.spark.SparkProgramRuntimeProvider;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.distributed.AbstractProgramTwillRunnable;
import co.cask.cdap.internal.app.spark.SparkCompat;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;

import javax.annotation.Nullable;

/**
 * A {@link TwillRunnable} wrapper for {@link ProgramRunner} that runs spark.
 */
final class SparkTwillRunnable extends AbstractProgramTwillRunnable<ProgramRunner> {

  SparkTwillRunnable(String name) {
    super(name);
  }

  @Override
  protected ProgramRunner createProgramRunner(Injector injector) {
    // Inside the TwillRunanble, we use the "Local" SparkRunner, since we need to actually submit the job.
    // The actual execution mode of the job is governed by the framework configuration,
    // which is in the hConf we shipped from DistributedSparkProgramRunner
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    return new SparkProgramRuntimeProvider(SparkCompat.get(cConf)) { }
      .createProgramRunner(ProgramType.SPARK, ProgramRuntimeProvider.Mode.LOCAL, injector);
  }

  @VisibleForTesting
  @Override
  protected Module createModule(CConfiguration cConf, Configuration hConf, TwillContext context,
                                ProgramId programId, String runId, String instanceId, @Nullable String principal) {
    Module module = super.createModule(cConf, hConf, context, programId, runId, instanceId, principal);
    return new AbstractModule() {
      @Override
      protected void configure() {
        install(module);
        install(new DistributedArtifactManagerModule());
      }
    };
  }
}

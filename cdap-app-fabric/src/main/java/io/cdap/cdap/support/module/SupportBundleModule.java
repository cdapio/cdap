/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.module;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.support.task.factory.SupportBundlePipelineInfoTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleSystemLogTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;

/**
 * Support bundle module to bind factories
 */
public class SupportBundleModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder<SupportBundleTaskFactory> supportBundleTaskFactoryMultibinder = Multibinder.newSetBinder(
      binder(), SupportBundleTaskFactory.class, Names.named(SupportBundle.TASK_FACTORY));
    supportBundleTaskFactoryMultibinder.addBinding().to(SupportBundlePipelineInfoTaskFactory.class);
    supportBundleTaskFactoryMultibinder.addBinding().to(SupportBundleSystemLogTaskFactory.class);
  }
}

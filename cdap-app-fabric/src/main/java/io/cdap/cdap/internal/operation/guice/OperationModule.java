/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation.guice;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.internal.app.sourcecontrol.LocalApplicationManager;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsOperation;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsOperationFactory;
import io.cdap.cdap.internal.app.sourcecontrol.PushAppsOperation;
import io.cdap.cdap.internal.app.sourcecontrol.PushAppsOperationFactory;
import io.cdap.cdap.internal.operation.InMemoryOperationRunner;
import io.cdap.cdap.internal.operation.InMemoryOperationRuntime;
import io.cdap.cdap.internal.operation.LongRunningOperation;
import io.cdap.cdap.internal.operation.MessagingOperationStatePublisher;
import io.cdap.cdap.internal.operation.OperationRunner;
import io.cdap.cdap.internal.operation.OperationRuntime;
import io.cdap.cdap.internal.operation.OperationStatePublisher;
import io.cdap.cdap.sourcecontrol.ApplicationManager;


/**
 *  Guice module for operation classes.
 */
public class OperationModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new FactoryModuleBuilder()
        .implement(LongRunningOperation.class, PullAppsOperation.class)
        .build(PullAppsOperationFactory.class));
    install(new FactoryModuleBuilder()
        .implement(LongRunningOperation.class, PushAppsOperation.class)
        .build(PushAppsOperationFactory.class));
    // TODO(samik) change based on worker enabled on not
    bind(ApplicationManager.class).to(LocalApplicationManager.class);
    bind(OperationRunner.class).to(InMemoryOperationRunner.class);
    bind(OperationStatePublisher.class).to(MessagingOperationStatePublisher.class);
    bind(OperationRuntime.class).to(InMemoryOperationRuntime.class);
  }
}


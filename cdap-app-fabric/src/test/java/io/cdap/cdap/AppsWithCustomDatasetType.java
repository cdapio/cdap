/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.AbstractDataset;
import io.cdap.cdap.api.dataset.module.EmbeddedDataset;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.workflow.AbstractWorkflow;

/**
 * Apps that declare a dataset type that is not defined by the system, for testing that such apps cannot be
 * deployed.
 */
public final class AppsWithCustomDatasetType {

  private AppsWithCustomDatasetType() {
    // no-op
  }

  /**
   * An app that adds a custom dataset type.
   */
  public static final class AppWithDatasetType extends AbstractApplication {

    @Override
    public void configure() {
      addDatasetType(CustomDataset.class);
      addWorker(new TestWorker());
    }
  }

  /**
   * An app that creates a dataset of a custom dataset class.
   */
  public static final class AppWithDatasetOfCustomClass extends AbstractApplication {

    @Override
    public void configure() {
      createDataset("custom", CustomDataset.class);
      addWorker(new TestWorker());
    }
  }

  /**
   * An app with a workflow that creates a local dataset of a custom dataset class.
   */
  public static final class AppWithLocalDatasetOfCustomClass extends AbstractApplication {

    @Override
    public void configure() {
      addWorkflow(new TestWorkflow());
    }
  }

  /**
   * A custom dataset for testing.
   */
  public static final class CustomDataset extends AbstractDataset {

    public CustomDataset(DatasetSpecification spec, @EmbeddedDataset("t") Table table) {
      super(spec.getName(), table);
    }
  }

  /**
   * A worker for testing.
   */
  public static final class TestWorker extends AbstractWorker {

    @Override
    public void run() {
      // no-op
    }
  }

  /**
   * A workflow for testing.
   */
  public static final class TestWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      createLocalDataset("custom", CustomDataset.class);
      addAction(new TestAction());
    }
  }

  /**
   * A custom action for testing.
   */
  public static final class TestAction extends AbstractCustomAction {

    @Override
    public void run() {
      // no-op
    }
  }
}

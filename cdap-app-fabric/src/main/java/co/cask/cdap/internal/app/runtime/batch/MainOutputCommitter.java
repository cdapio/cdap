/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputsCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;

import java.io.IOException;
import java.util.Map;

/**
 * An {@link OutputCommitter} used to
 */
public class MainOutputCommitter extends MultipleOutputsCommitter {

  public MainOutputCommitter(Map<String, OutputCommitter> committers) {
    super(committers);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    // start Tx
    super.setupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    // commitTx
  }
}

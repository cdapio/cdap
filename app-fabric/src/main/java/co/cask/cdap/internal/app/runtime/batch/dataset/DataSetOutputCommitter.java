/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

class DataSetOutputCommitter extends OutputCommitter {
  @Override
  public void setupJob(final JobContext jobContext) throws IOException {
    // DO NOTHING, see needsTaskCommit() comment
  }

  @Override
  public boolean needsTaskCommit(final TaskAttemptContext taskContext) throws IOException {
    // Don't do commit of individual task work. Work is committed on job level. Ops are flushed on a Mapper/Reducer
    // wrapper level
    return false;
  }

  @Override
  public void setupTask(final TaskAttemptContext taskContext) throws IOException {
    // DO NOTHING, see needsTaskCommit() comment
  }

  @Override
  public void commitTask(final TaskAttemptContext taskContext) throws IOException {
    // DO NOTHING, see needsTaskCommit() comment
  }

  @Override
  public void abortTask(final TaskAttemptContext taskContext) throws IOException {
    // DO NOTHING, see needsTaskCommit() comment
  }
}

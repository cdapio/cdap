/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import javax.annotation.Nullable;

/**
 * Interface designed for holding {@link PreviewJob}
 */
public interface PreviewJobQueue {
  /**
   * Poll the next available job in the queue.
   * @param runnerId id of the preview runner which is polling the job
   * @return {@code PreviewJob} if such job is available in the queue, {@code null} otherwise
   */
  @Nullable
  PreviewJob poll(String runnerId);

  /**
   * Add a preview job in the waiting queue.
   * @param previewJob the job to be added to the waiting queue
   * @return state of the job queue after addition of the current job
   */
  PreviewJobQueueState add(PreviewJob previewJob);
}

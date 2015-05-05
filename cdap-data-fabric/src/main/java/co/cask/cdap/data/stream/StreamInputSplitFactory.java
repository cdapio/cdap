/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.stream;

import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

/**
 * Factory for creating stream input splits. Generic because mapreduce and mapred apis are separate.
 *
 * @param <T> Type of input split to create. Expected to be mapred.InputSplit or mapreduce.InputSplit.
 */
public interface StreamInputSplitFactory<T> {

  /**
   * Create a stream input split.
   *
   * @param path path of the split
   * @param indexPath path to the index file for the split
   * @param startTime start timestamp for the split
   * @param endTime end timestamp for the split
   * @param start start position of the file
   * @param length length of the file
   * @param locations locations
   * @return stream input split
   */
  T createSplit(Path path, Path indexPath, long startTime, long endTime,
                long start, long length, @Nullable String[] locations);
}

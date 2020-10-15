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

package io.cdap.cdap.etl.spark.io;

import io.cdap.cdap.etl.spark.batch.SparkBytesReadCounter;
import io.cdap.cdap.etl.spark.batch.SparkBytesWrittenCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.net.URI;

/**
 * Implementation of {@link TaskAttemptContext} that support bytes read/written counter.
 */
final class TrackingTaskAttemptContext implements TaskAttemptContext {

  private final TaskAttemptContext delegate;

  TrackingTaskAttemptContext(TaskAttemptContext delegate) {
    this.delegate = delegate;
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return getCounter(counterName.getDeclaringClass().getName(), counterName.name());
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    if (FileInputFormatCounter.class.getName().equals(groupName)
      && FileInputFormatCounter.BYTES_READ.name().equals(counterName)) {
      return new SparkBytesReadCounter();
    }
    if (FileOutputFormatCounter.class.getName().equals(groupName)
      && FileOutputFormatCounter.BYTES_WRITTEN.name().equals(counterName)) {
      return new SparkBytesWrittenCounter();
    }

    return delegate.getCounter(groupName, counterName);
  }

  @Override
  public TaskAttemptID getTaskAttemptID() {
    return delegate.getTaskAttemptID();
  }

  @Override
  public void setStatus(String msg) {
    delegate.setStatus(msg);
  }

  @Override
  public String getStatus() {
    return delegate.getStatus();
  }

  @Override
  public float getProgress() {
    return delegate.getProgress();
  }

  @Override
  public Configuration getConfiguration() {
    return delegate.getConfiguration();
  }

  @Override
  public Credentials getCredentials() {
    return delegate.getCredentials();
  }

  @Override
  public JobID getJobID() {
    return delegate.getJobID();
  }

  @Override
  public int getNumReduceTasks() {
    return delegate.getNumReduceTasks();
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
    return delegate.getWorkingDirectory();
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return delegate.getOutputKeyClass();
  }

  @Override
  public Class<?> getOutputValueClass() {
    return delegate.getOutputValueClass();
  }

  @Override
  public Class<?> getMapOutputKeyClass() {
    return delegate.getMapOutputKeyClass();
  }

  @Override
  public Class<?> getMapOutputValueClass() {
    return delegate.getMapOutputValueClass();
  }

  @Override
  public String getJobName() {
    return delegate.getJobName();
  }

  @Override
  public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
    return delegate.getInputFormatClass();
  }

  @Override
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
    return delegate.getMapperClass();
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
    return delegate.getCombinerClass();
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
    return delegate.getReducerClass();
  }

  @Override
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
    return delegate.getOutputFormatClass();
  }

  @Override
  public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
    return delegate.getPartitionerClass();
  }

  @Override
  public RawComparator<?> getSortComparator() {
    return delegate.getSortComparator();
  }

  @Override
  public String getJar() {
    return delegate.getJar();
  }

  @Override
  public RawComparator<?> getCombinerKeyGroupingComparator() {
    return delegate.getCombinerKeyGroupingComparator();
  }

  @Override
  public RawComparator<?> getGroupingComparator() {
    return delegate.getGroupingComparator();
  }

  @Override
  public boolean getJobSetupCleanupNeeded() {
    return delegate.getJobSetupCleanupNeeded();
  }

  @Override
  public boolean getTaskCleanupNeeded() {
    return delegate.getTaskCleanupNeeded();
  }

  @Override
  public boolean getProfileEnabled() {
    return delegate.getProfileEnabled();
  }

  @Override
  public String getProfileParams() {
    return delegate.getProfileParams();
  }

  @Override
  public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
    return delegate.getProfileTaskRange(isMap);
  }

  @Override
  public String getUser() {
    return delegate.getUser();
  }

  @Override
  @Deprecated
  public boolean getSymlink() {
    return delegate.getSymlink();
  }

  @Override
  public Path[] getArchiveClassPaths() {
    return delegate.getArchiveClassPaths();
  }

  @Override
  public URI[] getCacheArchives() throws IOException {
    return delegate.getCacheArchives();
  }

  @Override
  public URI[] getCacheFiles() throws IOException {
    return delegate.getCacheFiles();
  }

  @Override
  @Deprecated
  public Path[] getLocalCacheArchives() throws IOException {
    return delegate.getLocalCacheArchives();
  }

  @Override
  @Deprecated
  public Path[] getLocalCacheFiles() throws IOException {
    return delegate.getLocalCacheFiles();
  }

  @Override
  public Path[] getFileClassPaths() {
    return delegate.getFileClassPaths();
  }

  @Override
  public String[] getArchiveTimestamps() {
    return delegate.getArchiveTimestamps();
  }

  @Override
  public String[] getFileTimestamps() {
    return delegate.getFileTimestamps();
  }

  @Override
  public int getMaxMapAttempts() {
    return delegate.getMaxMapAttempts();
  }

  @Override
  public int getMaxReduceAttempts() {
    return delegate.getMaxReduceAttempts();
  }

  @Override
  public void progress() {
    delegate.progress();
  }
}

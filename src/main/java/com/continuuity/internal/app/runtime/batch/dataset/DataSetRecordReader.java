package com.continuuity.internal.app.runtime.batch.dataset;


import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.SplitReader;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

class DataSetRecordReader extends RecordReader {
  private final SplitReader splitReader;
  private final BatchReadable dataset;

  public DataSetRecordReader(final BatchReadable dataset, final SplitReader splitReader) {
    this.dataset = dataset;
    this.splitReader = splitReader;
  }

  @Override
  public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException,
                                                                                          InterruptedException {
    DataSetInputSplit inputSplit = (DataSetInputSplit) split;

    try {
      splitReader.initialize(dataset, inputSplit.getSplit());
    } catch(OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    try {
      return splitReader.nextKeyValue();
    } catch(OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Object getCurrentKey() throws IOException, InterruptedException {
    return splitReader.getCurrentKey();
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    return splitReader.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return splitReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    splitReader.close();
  }
}

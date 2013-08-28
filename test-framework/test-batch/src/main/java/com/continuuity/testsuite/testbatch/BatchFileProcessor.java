package com.continuuity.testsuite.testbatch;


import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.batch.AbstractMapReduce;
import com.continuuity.api.batch.MapReduceContext;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.dataset.FileDataSet;
import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 *
 */
public class BatchFileProcessor extends AbstractMapReduce {
  @Override
  public MapReduceSpecification configure() {
    return MapReduceSpecification.Builder.with()
      .setName("BatchFileProcessor")
      .setDescription("Batch load file from disk")
      .useInputDataSet("key-value")
      .useOutputDataSet("file-stats")
      .build();
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = (Job) context.getHadoopJob();
    job.setMapperClass(FileMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(PerFileReducer.class);

    // LoadFile, store ib objectStore
    FileDataSet fileDataSet = new FileDataSet("trx-txt", URI.create("./data/trx.txt"));
    InputStream is = fileDataSet.getInputStream();
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    // Do something with Datasets here.
  }

  public static class FileMapper extends Mapper<byte[], FileStat, Text, Text> {

    @UseDataSet("key-value")
    FileDataSet fileDataSet;

    @Override
    public void map(byte[] key, FileStat fileStat, Context context)
      throws IOException, InterruptedException {
      String filename = fileStat.getFilename();
      fileStat.countLine(); // Count all words for this line
      context.write(new Text(filename), new Text(new Gson().toJson(fileStat)));
    }
  }

  public static class PerFileReducer extends Reducer<Text, Text, byte[], Text> {

    public void reduce(Text filename, Iterable<Text> values, Reducer.Context context)
      throws IOException, InterruptedException {

      long wordCount = 0;

      // Count Stats
      for (Text val : values) {
          FileStat fileStat = new Gson().fromJson(val.toString(), FileStat.class);
          wordCount += fileStat.getWordCount();
      }

      context.write(filename.toString(), wordCount);
    }
  }
}

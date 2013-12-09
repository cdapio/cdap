/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Customer;
import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Map reduce job that aggregates customer count per zip code.
 */
public class RegionBuilder extends AbstractMapReduce {


  @Override
  public MapReduceSpecification configure() {
    return MapReduceSpecification.Builder.with()
      .setName("RegionBuilder")
      .setDescription("Aggregator per zip code builder Map Reduce job")
      .useInputDataSet("customers")
      .useOutputDataSet("region")
      .build();
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = (Job) context.getHadoopJob();
    job.setMapperClass(RegionMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(RegionReducer.class);
  }

  public static class RegionMapper extends Mapper<byte[], Customer, Text, Text> {
    @Override
    public void map(byte[] key, Customer customer, Context context)
      throws IOException, InterruptedException {
      int zip = customer.getZip();
      context.write(new Text(String.valueOf(zip)), new Text(new Gson().toJson(customer)));
    }
  }

  public static class RegionReducer extends Reducer<Text, Text, byte[], byte[]> {

    public void reduce(Text zip, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

      int numCustomer = 0;
      // Very redundant, on purpose to keep the reducer busy.
      for (Text val : values) {
        numCustomer++;
      }
      context.write(Bytes.toBytes(zip.toString()), Bytes.toBytes(numCustomer));
    }
  }
}

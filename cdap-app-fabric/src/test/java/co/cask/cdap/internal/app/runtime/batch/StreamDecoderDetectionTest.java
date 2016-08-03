/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.data.stream.AbstractStreamInputFormat;
import co.cask.cdap.data.stream.decoder.IdentityStreamEventDecoder;
import co.cask.cdap.data.stream.decoder.TextStreamEventDecoder;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for stream decoder detection.
 */
public class StreamDecoderDetectionTest {

  @Test
  public void testDecoderDetection() throws IOException {
    Configuration hConf = new Configuration();

    hConf.setClass(Job.MAP_CLASS_ATTR, IdentityMapper.class, Mapper.class);
    AbstractStreamInputFormat.inferDecoderClass(hConf, MapReduceRuntimeService.getInputValueType(
      hConf, Void.class, getMapperTypeToken(hConf)));
    Assert.assertSame(IdentityStreamEventDecoder.class, AbstractStreamInputFormat.getDecoderClass(hConf));

    hConf.setClass(Job.MAP_CLASS_ATTR, NoTypeMapper.class, Mapper.class);
    AbstractStreamInputFormat.inferDecoderClass(hConf, MapReduceRuntimeService.getInputValueType(
      hConf, StreamEvent.class, getMapperTypeToken(hConf)));
    Assert.assertSame(IdentityStreamEventDecoder.class, AbstractStreamInputFormat.getDecoderClass(hConf));

    hConf.setClass(Job.MAP_CLASS_ATTR, TextMapper.class, Mapper.class);
    AbstractStreamInputFormat.inferDecoderClass(hConf, MapReduceRuntimeService.getInputValueType(
      hConf, Void.class, getMapperTypeToken(hConf)));
    Assert.assertSame(TextStreamEventDecoder.class, AbstractStreamInputFormat.getDecoderClass(hConf));

    try {
      hConf.setClass(Job.MAP_CLASS_ATTR, InvalidTypeMapper.class, Mapper.class);
      AbstractStreamInputFormat.inferDecoderClass(hConf, MapReduceRuntimeService.getInputValueType(
        hConf, Void.class, getMapperTypeToken(hConf)));
      Assert.fail("Expected Exception");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  private TypeToken<?> getMapperTypeToken(Configuration hConf) {
    return MapReduceRuntimeService.resolveClass(hConf, Job.MAP_CLASS_ATTR, Mapper.class);
  }

  public static final class IdentityMapper extends Mapper<LongWritable, StreamEvent, String, String> {
    // No-op
  }

  public static final class NoTypeMapper extends Mapper {
    // No-op
  }

  public static final class TextMapper extends Mapper<LongWritable, Text, String, String> {
    // No-op
  }

  public static final class InvalidTypeMapper<I, O> extends Mapper<I, O, String, String> {
    // No-op
  }
}

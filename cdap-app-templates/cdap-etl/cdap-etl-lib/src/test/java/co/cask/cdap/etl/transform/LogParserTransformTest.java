/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.transform;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.common.MockEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class LogParserTransformTest {
  private static final Schema STRING_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );
  private static final Schema BYTE_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("body", Schema.of(Schema.Type.BYTES))
  );
  private static final LogParserTransform.LogParserConfig S3_CONFIG =
    new LogParserTransform.LogParserConfig("S3", "body");
  private static final Transform<StructuredRecord, StructuredRecord> S3_TRANSFORM = new LogParserTransform(S3_CONFIG);
  private static final LogParserTransform.LogParserConfig CLF_CONFIG =
    new LogParserTransform.LogParserConfig("CLF", "body");
  private static final Transform<StructuredRecord, StructuredRecord> CLF_TRANSFORM =
    new LogParserTransform(CLF_CONFIG);
  private static final LogParserTransform.LogParserConfig CLOUDFRONT_CONFIG =
    new LogParserTransform.LogParserConfig("Cloudfront", "body");
  private static final Transform<StructuredRecord, StructuredRecord> CLOUDFRONT_TRANSFORM =
    new LogParserTransform(CLOUDFRONT_CONFIG);

  @Test
  public void testS3LogTransform() throws Exception {
    StructuredRecord botRecord = StructuredRecord.builder(STRING_SCHEMA)
      .set("body", "13a9f69e4a00effd6b4b891dcbcabef632ef9a9da7c localhost " +
        "[22/Jan/2015:11:03:21 +0000] 122.122.111.11 - 6006CA0AE4 REST.GET.OBJECT " +
        "ubuntu/this/is/some/folder " +
        "\"GET /my/uri.gif releases/dists/precise/releases/i18n/Translation-en HTTP/1.1\" " +
        "403 AccessDenied 231 - 10 - \"-\" \"Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.  17)\" -")
      .build();

    StructuredRecord browserRecord = StructuredRecord.builder(STRING_SCHEMA)
      .set("body", "13a9f69e4a00effd6b4b891dcef632ef9afe38cc8b0 localhost " +
             "[31/Jan/2015:21:57:57 +0000] 133.133.133.133 - 0E94306589 REST.GET.OBJECT " +
             "downloads/this/is/another/folder/with/a/file/file.zip " +
             "\"GET /my/uri.jpg HTTP/1.1\" 304 - - 195750039 198 - " +
             "\"-\" \"Mozilla/5.0 Gecko/20100115 Firefox/3.6\" -")
      .build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    S3_TRANSFORM.transform(botRecord, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);
    Assert.assertEquals("/my/uri.gif", output.get("uri"));
    Assert.assertEquals("122.122.111.11", output.get("ip"));
    Assert.assertEquals("unknown", output.get("browser"));
    Assert.assertEquals("", output.get("device"));
    Assert.assertEquals(403, output.get("httpStatus"));
    Assert.assertEquals(1421924601000L, output.get("ts"));

    S3_TRANSFORM.transform(browserRecord, emitter);
    output = emitter.getEmitted().get(1);
    Assert.assertEquals("/my/uri.jpg", output.get("uri"));
    Assert.assertEquals("133.133.133.133", output.get("ip"));
    Assert.assertEquals("Firefox", output.get("browser"));
    Assert.assertEquals("Personal computer", output.get("device"));
    Assert.assertEquals(304, output.get("httpStatus"));
    Assert.assertEquals(1422741477000L, output.get("ts"));
  }

  @Test
  public void testCloudfrontLogTransform() throws Exception {
    String event = "2015-04-17\t13:35:48\tSFO20\t582123\t11.111.111.11\tGET\texample.cloudfront" +
      ".net\t/coopr-standalone-vm/0.9.8/coopr-standalone-vm-0.9.8.ova\t200\t-\tMozilla/5" +
      ".0%2520(compatible;%2520Yahoo!%2520Slurp;%2520http://help.yahoo.com/help/us/ysearch/slurp)" +
      "\t-\tError\tsCmB94WPP5v-QoCyn7Jz1ZLn0kBhzIEkqfFuX2Gh5oA1SA8dsLp-kw==\texample.co\thttp\t264\t0.984";

    StructuredRecord record = StructuredRecord.builder(STRING_SCHEMA)
      .set("body", event)
      .build();

    String comment = "#Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status " +
      "cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header " +
      "cs-protocol cs-bytes time-taken";

    StructuredRecord commentRecord = StructuredRecord.builder(STRING_SCHEMA)
      .set("body", comment)
      .build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CLOUDFRONT_TRANSFORM.transform(record, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);
    Assert.assertEquals("/coopr-standalone-vm/0.9.8/coopr-standalone-vm-0.9.8.ova", output.get("uri"));
    Assert.assertEquals("11.111.111.11", output.get("ip"));
    Assert.assertEquals("unknown", output.get("browser"));
    Assert.assertEquals("", output.get("device"));
    Assert.assertEquals(200, output.get("httpStatus"));
    Assert.assertEquals(1429277748000L, output.get("ts"));

    CLOUDFRONT_TRANSFORM.transform(commentRecord, emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
  }

  @Test
  public void testCLFLogTransform() throws Exception {
    StructuredRecord record = StructuredRecord.builder(BYTE_SCHEMA)
      .set("body", ByteBuffer.wrap((Bytes.toBytes("127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] " +
                                                    "\"GET /apache_pb.gif HTTP/1.0\" 200 2326 " +
                                                    "\"http://www.example.com/start.html\" \"Mozilla/5.0 " +
                                                    "(Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 " +
                                                    "(KHTML, like Gecko) Chrome/43.0.2357.124 Safari/537.36\""))))
      .build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CLF_TRANSFORM.transform(record, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);
    Assert.assertEquals("/apache_pb.gif", output.get("uri"));
    Assert.assertEquals("127.0.0.1", output.get("ip"));
    Assert.assertEquals("Chrome", output.get("browser"));
    Assert.assertEquals("Personal computer", output.get("device"));
    Assert.assertEquals(200, output.get("httpStatus"));
    Assert.assertEquals(971211336000L, output.get("ts"));
  }
}

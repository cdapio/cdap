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

package co.cask.cdap.io;

import co.cask.cdap.common.io.ByteBuffers;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Unit-test for {@link ByteBuffers} methods.
 */
public class ByteBuffersTest {

  @Test
  public void testWriteArrayByteBuffer() throws IOException {
    String content = "0123456789";
    testWriteBuffer(ByteBuffer.wrap(content.getBytes(Charsets.UTF_8)), content);
  }

  @Test
  public void testWriteDirectByteBuffer() throws IOException {
    String content = "0123456789";
    ByteBuffer buffer = ByteBuffer.allocateDirect(20);
    buffer.put(content.getBytes(Charsets.UTF_8));
    buffer.flip();
    testWriteBuffer(buffer, content);

    // Test large content
    buffer = ByteBuffer.allocateDirect(128 * 1000);
    content = Strings.repeat(content, buffer.remaining() / content.length());
    buffer.put(content.getBytes(Charsets.UTF_8));
    buffer.flip();
    testWriteBuffer(buffer, content);
  }

  private void testWriteBuffer(ByteBuffer buffer, String content) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    // Write the whole buffer
    ByteBuffers.writeToStream(buffer, os);
    Assert.assertEquals(content, new String(os.toByteArray(), Charsets.UTF_8));

    // Write part of the buffer
    int pos = content.length() / 2;
    buffer.position(pos);
    os.reset();
    ByteBuffers.writeToStream(buffer, os);
    Assert.assertEquals(content.substring(pos), new String(os.toByteArray(), Charsets.UTF_8));

    // Write with a slice of the buffer
    pos = content.length() / 3;
    buffer.position(pos);
    os.reset();
    ByteBuffers.writeToStream(buffer.slice(), os);
    Assert.assertEquals(content.substring(pos), new String(os.toByteArray(), Charsets.UTF_8));
  }
}

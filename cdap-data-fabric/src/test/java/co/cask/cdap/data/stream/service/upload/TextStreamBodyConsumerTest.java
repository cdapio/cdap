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

package co.cask.cdap.data.stream.service.upload;

import co.cask.cdap.api.common.Bytes;
import co.cask.http.BodyConsumer;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Unit test for {@link TextStreamBodyConsumer}.
 */
public class TextStreamBodyConsumerTest extends StreamBodyConsumerTestBase {

  @Override
  protected ContentInfo generateFile(final int recordCount) throws IOException {
    return new FileContentInfo(generateFile(TMP_FOLDER.newFile(), recordCount)) {

      @Override
      public boolean verify(Map<String, String> headers,
                            InputSupplier<? extends InputStream> contentSupplier) throws IOException {

        byte[] buf = null;
        InputStream input = contentSupplier.getInput();
        try {
          for (int i = 0; i < recordCount; i++) {
            byte[] expected = ("Message number " + i).getBytes(Charsets.UTF_8);
            buf = ensureCapacity(buf, expected.length);
            ByteStreams.readFully(input, buf, 0, expected.length);
            if (Bytes.compareTo(expected, 0, expected.length, buf, 0, expected.length) != 0) {
              return false;
            }
          }
          return true;
        } finally {
          input.close();
        }
      }
    };
  }

  @Override
  protected BodyConsumer createBodyConsumer(ContentWriterFactory contentWriterFactory) {
    return new TextStreamBodyConsumer(contentWriterFactory);
  }

  private File generateFile(File file, int recordCount) throws IOException {
    BufferedWriter writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      for (int i = 0; i < recordCount; i++) {
        writer.write("Message number " + i);

        // Intentionally not have the last end-of-line
        if (i != (recordCount - 1)) {
          writer.newLine();
        }
      }
      return file;
    } finally {
      writer.close();
    }
  }

  private byte[] ensureCapacity(@Nullable byte[] buf, int size) {
    if (buf != null && buf.length >= size) {
      return buf;
    }
    return new byte[size];
  }
}

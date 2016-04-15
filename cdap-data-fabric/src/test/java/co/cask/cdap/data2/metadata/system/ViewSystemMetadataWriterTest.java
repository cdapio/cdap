/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamViewId;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Tests for {@link ViewSystemMetadataWriter}.
 */
public class ViewSystemMetadataWriterTest {
  @Test
  public void testViewDefaultSchema() {
    Assert.assertNull(getDefaultViewMetadataSchema(Formats.AVRO));
    for (String format : new String[]{Formats.COMBINED_LOG_FORMAT, Formats.CSV, Formats.GROK, Formats.SYSLOG,
      Formats.TEXT, Formats.TSV}) {
      Assert.assertNotNull(getDefaultViewMetadataSchema(format));
    }
  }

  @Test
  public void testViewNonDefaultSchema() {
    Schema schema = Schema.recordOf("myschema", Schema.Field.of("myfield", Schema.of(Schema.Type.STRING)));
    for (String format : Formats.ALL) {
      Assert.assertEquals(schema.toString(), getViewMetadataSchema(format, schema));
    }
  }

  private String getDefaultViewMetadataSchema(String format) {
    return getViewMetadataSchema(format, null);
  }

  private String getViewMetadataSchema(String format, @Nullable Schema schema) {
    StreamViewId viewId = NamespaceId.DEFAULT.stream("mystream").view("myview");
    FormatSpecification formatSpec = new FormatSpecification(format, schema);
    ViewSpecification viewSpec = new ViewSpecification(formatSpec);
    NoOpMetadataStore metadataStore = new NoOpMetadataStore();
    ViewSystemMetadataWriter writer = new ViewSystemMetadataWriter(metadataStore, viewId.toId(), viewSpec);
    return writer.getSchemaToAdd();
  }
}

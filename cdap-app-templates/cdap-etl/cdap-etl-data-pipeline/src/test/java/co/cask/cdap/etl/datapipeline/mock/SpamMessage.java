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

package co.cask.cdap.etl.datapipeline.mock;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

/**
 * Represents a string and whether that is spam or not.
 */
public class SpamMessage {
  static final String IS_SPAM_FIELD = "isSpam";
  static final String TEXT_FIELD = "text";
  static final Schema SCHEMA = Schema.recordOf(
    "simpleMessage",
    Schema.Field.of(IS_SPAM_FIELD, Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of(TEXT_FIELD, Schema.of(Schema.Type.STRING))
  );

  private final boolean isSpam;
  private final String text;

  public SpamMessage(boolean isSpam, String text) {
    this.isSpam = isSpam;
    this.text = text;
  }

  public StructuredRecord toStructuredRecord() {
    return StructuredRecord.builder(SCHEMA).set(IS_SPAM_FIELD, isSpam).set(TEXT_FIELD, text).build();
  }
}

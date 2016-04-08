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

package co.cask.cdap.datapipeline.mock;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import javax.annotation.Nullable;

/**
 * Represents a string and whether that is spam or not.
 */
public class SpamMessage {
  public static final String SPAM_PREDICTION_FIELD = "isSpam";
  public static final String TEXT_FIELD = "text";
  static final Schema SCHEMA = Schema.recordOf(
    "simpleMessage",
    Schema.Field.of(SPAM_PREDICTION_FIELD, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of(TEXT_FIELD, Schema.of(Schema.Type.STRING))
  );

  private final String text;
  @Nullable
  private final Double spamPrediction;

  public SpamMessage(String text) {
    this(text, null);
  }

  public SpamMessage(String text, @Nullable Double spamPrediction) {
    this.text = text;
    this.spamPrediction = spamPrediction;
  }

  public StructuredRecord toStructuredRecord() {
    StructuredRecord.Builder builder = StructuredRecord.builder(SCHEMA);
    builder.set(TEXT_FIELD, text);
    if (spamPrediction != null) {
      builder.set(SPAM_PREDICTION_FIELD, spamPrediction);
    }
    return builder.build();
  }

  public static SpamMessage fromStructuredRecord(StructuredRecord structuredRecord) {
    return new SpamMessage((String) structuredRecord.get(TEXT_FIELD),
                           (Double) structuredRecord.get(SPAM_PREDICTION_FIELD));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SpamMessage that = (SpamMessage) o;

    if (!text.equals(that.text)) {
      return false;
    }
    return !(spamPrediction != null ? !spamPrediction.equals(that.spamPrediction) : that.spamPrediction != null);

  }

  @Override
  public int hashCode() {
    int result = text.hashCode();
    result = 31 * result + (spamPrediction != null ? spamPrediction.hashCode() : 0);
    return result;
  }
}

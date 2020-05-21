/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.join;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link JoinDefinition} and related builders.
 */
public class JoinDefinitionTest {
  private static final Schema USER_SCHEMA = Schema.recordOf(
    "user",
    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("age", Schema.of(Schema.Type.INT)),
    Schema.Field.of("bday", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))));
  private static final Schema PURCHASE_SCHEMA = Schema.recordOf(
    "purchase",
    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("user_id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("ts", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("coupon", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));

  @Test
  public void testInnerJoinSchema() {
    Schema expected = Schema.recordOf(
      "userPurchase",
      Schema.Field.of("purchase_id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("user_id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("ts", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("coupon", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("age", Schema.of(Schema.Type.INT)),
      Schema.Field.of("bday", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))));

    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA)
      .isRequired()
      .build();

    JoinStage users = JoinStage.builder("users", USER_SCHEMA)
      .isRequired()
      .build();

    testUserPurchaseSchema(purchases, users, expected);
  }

  @Test
  public void testLeftOuterJoinSchema() {
    Schema expected = Schema.recordOf(
      "userPurchase",
      Schema.Field.of("purchase_id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("user_id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("ts", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("coupon", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("bday", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))));

    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA)
      .isRequired()
      .build();

    JoinStage users = JoinStage.builder("users", USER_SCHEMA)
      .isOptional()
      .build();

    testUserPurchaseSchema(purchases, users, expected);
  }

  @Test
  public void testOuterJoinSchema() {
    Schema expected = Schema.recordOf(
      "userPurchase",
      Schema.Field.of("purchase_id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("user_id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("ts", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("price", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("coupon", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("bday", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))));

    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA)
      .isOptional()
      .build();

    JoinStage users = JoinStage.builder("users", USER_SCHEMA)
      .isOptional()
      .build();

    testUserPurchaseSchema(purchases, users, expected);
  }

  @Test
  public void testUnknownSchema() {
    JoinStage purchases = JoinStage.builder("purchases", null).build();

    JoinStage users = JoinStage.builder("users", null).build();

    JoinDefinition definition = JoinDefinition.builder()
      .select(new JoinField("purchases", "id", "purchase_id"),
              new JoinField("users", "id", "user_id"),
              new JoinField("users", "name"))
      .from(purchases, users)
      .on(JoinCondition.onKeys()
            .addKey(new JoinKey("purchases", Collections.singletonList("user_id")))
            .addKey(new JoinKey("users", Collections.singletonList("id")))
            .build())
      .build();

    Assert.assertNull(definition.getOutputSchema());
  }

  @Test
  public void testSelectMissingFieldThrowsException() {
    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA).build();
    JoinStage users = JoinStage.builder("users", USER_SCHEMA).build();

    try {
      JoinDefinition.builder()
        .select(new JoinField("purchases", "id", "purchase_id"),
                new JoinField("users", "abcdef"))
        .from(purchases, users)
        .on(JoinCondition.onKeys()
              .addKey(new JoinKey("purchases", Collections.singletonList("user_id")))
              .addKey(new JoinKey("users", Collections.singletonList("id")))
              .build())
        .build();
      Assert.fail("Select missing field did not fail as expected");
    } catch (InvalidJoinException e) {
      // expected
    }
  }

  @Test
  public void testSelectMissingStageThrowsException() {
    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA).build();
    JoinStage users = JoinStage.builder("users", USER_SCHEMA).build();

    try {
      JoinDefinition.builder()
        .select(new JoinField("purchases", "id", "purchase_id"),
                new JoinField("users2", "id"))
        .from(purchases, users)
        .on(JoinCondition.onKeys()
              .addKey(new JoinKey("purchases", Collections.singletonList("user_id")))
              .addKey(new JoinKey("users", Collections.singletonList("id")))
              .build())
        .build();
      Assert.fail("Select missing stage did not fail as expected");
    } catch (InvalidJoinException e) {
      // expected
    }
  }

  @Test
  public void testDuplicateOutputFieldsThrowsException() {
    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA).build();
    JoinStage users = JoinStage.builder("users", USER_SCHEMA).build();

    try {
      JoinDefinition.builder()
        .select(new JoinField("purchases", "user_id"),
                new JoinField("users", "id", "user_id"))
        .from(purchases, users)
        .on(JoinCondition.onKeys()
              .addKey(new JoinKey("purchases", Collections.singletonList("user_id")))
              .addKey(new JoinKey("users", Collections.singletonList("id")))
              .build())
        .build();
      Assert.fail("Duplicate fields did not fail as expected");
    } catch (InvalidJoinException e) {
      // expected
    }
  }

  @Test
  public void testWrongJoinKeyStageThrowsException() {
    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA).build();
    JoinStage users = JoinStage.builder("users", USER_SCHEMA).build();

    try {
      JoinDefinition.builder()
        .select(new JoinField("purchases", "id"),
                new JoinField("users", "id", "user_id"))
        .from(purchases, users)
        .on(JoinCondition.onKeys()
              .addKey(new JoinKey("abc", Collections.singletonList("user_id")))
              .addKey(new JoinKey("users", Collections.singletonList("id")))
              .build())
        .build();
      Assert.fail("Invalid join condition did not fail as expected");
    } catch (InvalidJoinConditionException e) {
      // expected
    }
  }

  @Test
  public void testJoinKeyMissingFieldThrowsException() {
    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA).build();
    JoinStage users = JoinStage.builder("users", USER_SCHEMA).build();

    try {
      JoinDefinition.builder()
        .select(new JoinField("purchases", "id"),
                new JoinField("users", "id", "user_id"))
        .from(purchases, users)
        .on(JoinCondition.onKeys()
              .addKey(new JoinKey("purchases", Collections.singletonList("abc")))
              .addKey(new JoinKey("users", Collections.singletonList("email")))
              .build())
        .build();
      Assert.fail("Invalid join condition did not fail as expected");
    } catch (InvalidJoinConditionException e) {
      // expected
    }
  }

  @Test
  public void testJoinKeyMismatchedNumFieldsThrowsException() {
    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA).build();
    JoinStage users = JoinStage.builder("users", USER_SCHEMA).build();

    try {
      JoinDefinition.builder()
        .select(new JoinField("purchases", "id"),
                new JoinField("users", "id", "user_id"))
        .from(purchases, users)
        .on(JoinCondition.onKeys()
              .addKey(new JoinKey("purchases", Arrays.asList("id", "user_id")))
              .addKey(new JoinKey("users", Collections.singletonList("id")))
              .build())
        .build();
      Assert.fail("Invalid join condition did not fail as expected");
    } catch (InvalidJoinConditionException e) {
      // expected
    }
  }

  @Test
  public void testJoinKeyMismatchedFieldTypeThrowsException() {
    JoinStage purchases = JoinStage.builder("purchases", PURCHASE_SCHEMA).build();
    JoinStage users = JoinStage.builder("users", USER_SCHEMA).build();

    try {
      JoinDefinition.builder()
        .select(new JoinField("purchases", "id"),
                new JoinField("users", "id", "user_id"))
        .from(purchases, users)
        .on(JoinCondition.onKeys()
              .addKey(new JoinKey("purchases", Arrays.asList("id")))
              .addKey(new JoinKey("users", Collections.singletonList("email")))
              .build())
        .build();
      Assert.fail("Invalid join condition did not fail as expected");
    } catch (InvalidJoinConditionException e) {
      // expected
    }
  }

  private void testUserPurchaseSchema(JoinStage purchases, JoinStage users, Schema expected) {
    JoinDefinition definition = JoinDefinition.builder()
      .select(new JoinField("purchases", "id", "purchase_id"),
              new JoinField("users", "id", "user_id"),
              new JoinField("purchases", "ts"),
              new JoinField("purchases", "price"),
              new JoinField("purchases", "coupon"),
              new JoinField("users", "name"),
              new JoinField("users", "email"),
              new JoinField("users", "age"),
              new JoinField("users", "bday"))
      .from(purchases, users)
      .on(JoinCondition.onKeys()
            .addKey(new JoinKey("purchases", Collections.singletonList("user_id")))
            .addKey(new JoinKey("users", Collections.singletonList("id")))
            .build())
      .setOutputSchemaName(expected.getRecordName())
      .build();

    Assert.assertEquals(expected, definition.getOutputSchema());
  }
}

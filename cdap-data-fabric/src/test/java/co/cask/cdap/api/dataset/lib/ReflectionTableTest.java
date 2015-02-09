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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.internal.io.ReflectionPutWriter;
import co.cask.cdap.internal.io.ReflectionRowReader;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ReflectionTableTest extends AbstractDatasetTest {
  private static final User SAMUEL = new User(
    "Samuel L.", "Jackson",
    Gender.MALE,
    123,
    1234567890000L,
    new Date(2015, 2, 10),
    50000000.02f,
    Double.MAX_VALUE,
    new int[] { 2, 8, 1, 3, 3, 0, 8, 0, 0, 4 },
    Lists.newArrayList("Shaft", "Mace", "Pulp Fiction guy"),
    ImmutableMap.of("occupation", "goat", "rank", "1"),
    new byte[]{0, 1, 2});

  public static enum Gender {
    MALE,
    FEMALE
  }

  public static class Date {
    private int year;
    private int month;
    private int day;

    public Date(int year, int month, int day) {
      this.year = year;
      this.month = month;
      this.day = day;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Date)) {
        return false;
      }

      Date that = (Date) o;

      return year == that.year && month == that.month && day == that.day;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(year, month, day);
    }
  }

  public static class User {
    private String firstName;
    private String lastName;
    private Gender gender;
    private Integer id;
    private Long timestamp;
    private Date signupDate;
    private Float salary;
    private Double lastPurchase;
    private int[] digits;
    private List<String> aliases;
    private Map<String, String> attributes;
    private byte[] blob;

    public User(String firstName, String lastName, Gender gender, Integer id, Long timestamp,
                Date signupDate, Float salary, Double lastPurchase, int[] digits,
                List<String> aliases, Map<String, String> attributes, byte[] blob) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.gender = gender;
      this.id = id;
      this.timestamp = timestamp;
      this.signupDate = signupDate;
      this.salary = salary;
      this.lastPurchase = lastPurchase;
      this.digits = digits;
      this.aliases = aliases;
      this.attributes = attributes;
      this.blob = blob;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof User)) {
        return false;
      }

      User that = (User) o;

      return Objects.equal(firstName, that.firstName) &&
        Objects.equal(lastName, that.lastName) &&
        Objects.equal(gender, that.gender) &&
        Objects.equal(id, that.id) &&
        Objects.equal(timestamp, that.timestamp) &&
        Objects.equal(signupDate, that.signupDate) &&
        Objects.equal(salary, that.salary) &&
        Objects.equal(lastPurchase, that.lastPurchase) &&
        Arrays.equals(digits, that.digits) &&
        Objects.equal(aliases, that.aliases) &&
        Objects.equal(attributes, that.attributes) &&
        Arrays.equals(blob, that.blob);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(firstName, lastName, gender, id, timestamp, signupDate, salary,
                              lastPurchase, digits, aliases, attributes, blob);
    }
  }

  public static class User2 {
    private String firstName;
    private Long id;
    private Double salary;
    private Double lastPurchase;
    private ByteBuffer blob;
    private Double newField;

    private User2(String firstName, Long id, Double salary, Double lastPurchase, ByteBuffer blob) {
      this.firstName = firstName;
      this.id = id;
      this.salary = salary;
      this.lastPurchase = lastPurchase;
      this.blob = blob;
      this.newField = null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof User2)) {
        return false;
      }

      User2 that = (User2) o;

      return Objects.equal(firstName, that.firstName) &&
        Objects.equal(id, that.id) &&
        Objects.equal(salary, that.salary) &&
        Objects.equal(lastPurchase, that.lastPurchase) &&
        Objects.equal(blob, that.blob) &&
        Objects.equal(newField, that.newField);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(firstName, id, salary, lastPurchase, blob, newField);
    }
  }

  @Test
  public void testPutAndGet() throws Exception {
    createInstance("table", "users", DatasetProperties.builder().build());
    try {
      final Table usersTable = getInstance("users");
      final byte[] rowKey = Bytes.toBytes(123);
      final Schema schema = new ReflectionSchemaGenerator().generate(User.class);
      assertGetAndPut(usersTable, rowKey, SAMUEL, schema);
    } finally {
      deleteInstance("users");
    }
  }

  @Test
  public void testNullFields() throws Exception {
    createInstance("table", "users", DatasetProperties.builder().build());
    try {
      final Table usersTable = getInstance("users");
      final byte[] rowKey = Bytes.toBytes(123);
      final Schema schema = new ReflectionSchemaGenerator().generate(User.class);
      assertGetAndPut(usersTable, rowKey, SAMUEL, schema);
    } finally {
      deleteInstance("users");
    }
  }

  @Test
  public void testTypeProjection() throws Exception {
    createInstance("table", "users", DatasetProperties.builder().build());
    try {
      final Table usersTable = getInstance("users");
      final byte[] rowKey = Bytes.toBytes(123);
      final User2 projected = new User2("Samuel L.", 123L, ((Float) 50000000.02f).doubleValue(), Double.MAX_VALUE,
                                        ByteBuffer.wrap(new byte[]{0, 1, 2}));
      final Schema fullSchema = new ReflectionSchemaGenerator().generate(User.class);
      final Schema projSchema = new ReflectionSchemaGenerator().generate(User2.class);

      // TableDataset is not accessible here, but we know that's the underlying implementation...
      TransactionExecutor tx = newTransactionExecutor((TransactionAware) usersTable);
      tx.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Put put = new Put(rowKey);
          ReflectionPutWriter putWriter = new ReflectionPutWriter(put);
          putWriter.write(SAMUEL, fullSchema);
          usersTable.put(put);
          Row row = usersTable.get(rowKey);
          ReflectionRowReader<User2> rowReader = new ReflectionRowReader<User2>(row);
          User2 actual = rowReader.read(fullSchema, projSchema, TypeToken.of(User2.class));
          Assert.assertEquals(projected, actual);
        }
      });
    } finally {
      deleteInstance("users");
    }
  }

  private void assertGetAndPut(final Table table, final byte[] rowKey, final Object obj,
                               final Schema schema) throws Exception {
    // TableDataset is not accessible here, but we know that's the underlying implementation...
    TransactionExecutor tx = newTransactionExecutor((TransactionAware) table);
    tx.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Put put = new Put(rowKey);
        ReflectionPutWriter putWriter = new ReflectionPutWriter(put);
        putWriter.write(obj, schema);
        table.put(put);
        Row row = table.get(rowKey);
        ReflectionRowReader<User> rowReader = new ReflectionRowReader<User>(row);
        User actual = rowReader.read(schema, schema, TypeToken.of(User.class));
        Assert.assertEquals(obj, actual);
      }
    });
  }
}

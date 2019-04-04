/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.transaction;

import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests exception propagation.
 */
public class TransactionRunnersTest {
  private static final TransactionRunner MOCK = runnable -> {
    try {
      runnable.run(null);
    } catch (Exception e) {
      throw new TransactionException(e.getMessage(), e);
    }
  };

  @Test
  public void testRuntimeExceptionPropagation() {
    try {
      TransactionRunners.run(MOCK, (TxRunnable) context -> {
        throw new IllegalArgumentException();
      });
      Assert.fail("runnable should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }


    try {
      TransactionRunners.run(MOCK, (TxCallable<Object>) context -> {
        throw new IllegalArgumentException();
      });
      Assert.fail("runnable should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testSingleExceptionCheckedPropagation() {
    try {
      TransactionRunners.run(MOCK, (TxRunnable) context -> {
        throw new TableNotFoundException(new StructuredTableId("id"));
      }, TableNotFoundException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (TableNotFoundException e) {
      // expected
    }


    try {
      TransactionRunners.run(MOCK, (TxCallable<Object>) context -> {
        throw new TableNotFoundException(new StructuredTableId("id"));
      }, TableNotFoundException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (TableNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testSingleExceptionRuntimePropagation() throws Exception {
    try {
      TransactionRunners.run(MOCK, (TxRunnable) context -> {
        throw new IllegalArgumentException();
      }, TableNotFoundException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }


    try {
      TransactionRunners.run(MOCK, (TxCallable<Object>) context -> {
        throw new IllegalArgumentException();
      }, TableNotFoundException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testTwoExceptionCheckedPropagation() throws Exception {
    try {
      TransactionRunners.run(MOCK, (TxRunnable) context -> {
        throw new TableAlreadyExistsException(new StructuredTableId("id"));
      }, TableNotFoundException.class, TableAlreadyExistsException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (TableAlreadyExistsException e) {
      // expected
    }


    try {
      TransactionRunners.run(MOCK, (TxCallable<Object>) context -> {
        throw new TableAlreadyExistsException(new StructuredTableId("id"));
      }, TableNotFoundException.class, TableAlreadyExistsException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (TableAlreadyExistsException e) {
      // expected
    }
  }

  @Test
  public void testTwoExceptionRuntimePropagation() throws Exception {
    try {
      TransactionRunners.run(MOCK, (TxRunnable) context -> {
        throw new IllegalArgumentException();
      }, TableNotFoundException.class, TableAlreadyExistsException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }


    try {
      TransactionRunners.run(MOCK, (TxCallable<Object>) context -> {
        throw new IllegalArgumentException();
      }, TableNotFoundException.class, TableAlreadyExistsException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testThreeExceptionCheckedPropagation() throws Exception {
    try {
      TransactionRunners.run(MOCK, context -> {
        throw new IOException("io");
      }, TableNotFoundException.class, TableAlreadyExistsException.class, IOException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testThreeExceptionRuntimePropagation() throws Exception {
    try {
      TransactionRunners.run(MOCK, context -> {
        throw new IllegalArgumentException();
      }, TableNotFoundException.class, TableAlreadyExistsException.class, IOException.class);
      Assert.fail("runnable should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}

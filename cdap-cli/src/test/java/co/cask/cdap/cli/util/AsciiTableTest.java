/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.cli.util;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Test;

/**
 * A test for testing {@link AsciiTable} formatting. Not run as part of unit-test.
 * Just to have it handy to test format in IDE.
 */
@Ignore
public class AsciiTableTest {

  @Test
  public void testFormat() {
    new AsciiTable<String>(
      new String[] {"c1", "c2", "c3333"},
      ImmutableList.of(
        "r1\n456,r11,r1",
        "r2,r2222\n123,r",
        "r3333,r3,r3\n1"
      ),
      new RowMaker<String>() {
        @Override
        public Object[] makeRow(String object) {
          return object.split(",");
        }
      }
    ).print(System.out);
  }

  @Test
  public void testBigCell() {
    new AsciiTable<String>(
      new String[] {"c1", "c2", "c3333"},
      ImmutableList.of(
        "r1zz" + Strings.repeat("z", 300) + "456,r11,r1",
        "r2,r2222 zzzzzzz z z z zzzzzz z zzzzzzzzz zzzzzzz zzzzzzz zzzzzzz zzzzz zzz123,r",
        "r3333,r3,r3\n1"
      ),
      new RowMaker<String>() {
        @Override
        public Object[] makeRow(String object) {
          return object.split(",");
        }
      }
    ).print(System.out);
  }

  @Test
  public void testTwoLineCell() {
    new AsciiTable<String>(
      new String[] {"c1", "c2", "c3333"},
      ImmutableList.of(
        "123456789012345678901234567890,2,3",
        "r2,r2222 z",
        "r3333,r3,r3\n1"
      ),
      new RowMaker<String>() {
        @Override
        public Object[] makeRow(String object) {
          return object.split(",");
        }
      }
    ).print(System.out);
  }

  @Test
  public void testTwoLineCell2() {
    new AsciiTable<String>(
      new String[] {"c1", "c2", "c3333"},
      ImmutableList.of(
        Strings.repeat("z", 27) + "a,2,3",
        "r2,r2222 z",
        "r3333,r3,r3\n1"
      ),
      new RowMaker<String>() {
        @Override
        public Object[] makeRow(String object) {
          return object.split(",");
        }
      }
    ).print(System.out);
  }
}

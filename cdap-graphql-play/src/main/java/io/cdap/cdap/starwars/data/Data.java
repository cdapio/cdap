/*
 *
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

package io.cdap.cdap.starwars.data;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * TODO
 */
public class Data {

  private static final Map<String, Object> THREEPIO = ImmutableMap.of(
    "id", "2000",
    "name", "C-3PO",
    "friends", Arrays.asList("1000", "1002", "1003", "2001"),
    "appearsIn", Arrays.asList(4, 5, 6),
    "primaryFunction", "Protocol"
  );

  private static final Map<String, Object> ARTOO = ImmutableMap.of(
    "id", "2001",
    "name", "R2-D2",
    "friends", Arrays.asList("1000", "1002", "1003"),
    "appearsIn", Arrays.asList(4, 5, 6),
    "primaryFunction", "Astromech"
  );
  public static final Map<String, Map<String, Object>> DROIDS = ImmutableMap.of(
    "2000", THREEPIO,
    "2001", ARTOO
  );
  private static final Map<String, Object> HAN = ImmutableMap.of(
    "id", "1002",
    "name", "Han Solo",
    "friends", Arrays.asList("1000", "1003", "2001"),
    "appearsIn", Arrays.asList(4, 5, 6)
  );
  private static final Map<String, Object> LEIA = ImmutableMap.of(
    "id", "1003",
    "name", "Leia Organa",
    "friends", Arrays.asList("1000", "1002", "2000", "2001"),
    "appearsIn", Arrays.asList(4, 5, 6),
    "homePlanet", "Alderaan"
  );
  private static final Map<String, Object> TARKIN = ImmutableMap.of(
    "id", "1004",
    "name", "Wilhuff Tarkin",
    "friends", Collections.singletonList("1001"),
    "appearsIn", Arrays.asList(4)
  );
  private static final Map<String, Object> LUKE = ImmutableMap.of(
    "id", "1000",
    "name", "Luke Skywalker",
    "friends", Arrays.asList("1002", "1003", "2000", "2001"),
    "appearsIn", Arrays.asList(4, 5, 6),
    "homePlanet", "Tatooine"
  );
  private static final Map<String, Object> VADER = ImmutableMap.of(
    "id", "1001",
    "name", "Darth Vader",
    "friends", Arrays.asList("1004"),
    "appearsIn", Arrays.asList(4, 5, 6),
    "homePlanet", "Tatooine"
  );
  public static final Map<String, Map<String, Object>> HUMANS = ImmutableMap.of(
    "1000", LUKE,
    "1001", VADER,
    "1002", HAN,
    "1003", LEIA,
    "1004", TARKIN
  );

}

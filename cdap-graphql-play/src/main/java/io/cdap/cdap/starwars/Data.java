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

package io.cdap.cdap.starwars;

import com.google.common.collect.ImmutableMap;
import com.sun.tools.javac.util.List;

import java.util.Map;

public class Data {

  static final Map<String, Object> LUKE = ImmutableMap.of(
    "id", "1000",
    "name", "Luke Skywalker",
    "friends", List.of("1002", "1003", "2000", "2001"),
    "appearsIn", List.of(4, 5, 6),
    "homePlanet", "Tatooine"
  );
  // static def vader = [
  // id        : '1001',
  // name      : 'Darth Vader',
  // friends   : ['1004'],
  // appearsIn : [4, 5, 6],
  // homePlanet: 'Tatooine',
  //   ]
  //
  // static def han = [
  // id       : '1002',
  // name     : 'Han Solo',
  // friends  : ['1000', '1003', '2001'],
  // appearsIn: [4, 5, 6],
  //   ]
  //
  // static def leia = [
  // id        : '1003',
  // name      : 'Leia Organa',
  // friends   : ['1000', '1002', '2000', '2001'],
  // appearsIn : [4, 5, 6],
  // homePlanet: 'Alderaan',
  //   ]
  //
  // static def tarkin = [
  // id       : '1004',
  // name     : 'Wilhuff Tarkin',
  // friends  : ['1001'],
  // appearsIn: [4],
  //   ]


  static final Map<String, String> HUMANS = ImmutableMap.of(
    "1000", "luke"
    //   '1001': vader,
    //   '1002': han,
    //   '1003': leia,
    //   '1004': tarkin,
  );

  // static def threepio = [
  // id             : '2000',
  // name           : 'C-3PO',
  // friends        : ['1000', '1002', '1003', '2001'],
  // appearsIn      : [4, 5, 6],
  // primaryFunction: 'Protocol',
  //   ]
  //
  // static def artoo = [
  // id             : '2001',
  // name           : 'R2-D2',
  // friends        : ['1000', '1002', '1003'],
  // appearsIn      : [4, 5, 6],
  // primaryFunction: 'Astromech',
  //   ]
  //
  // static def droidData = [
  //   "2000": threepio,
  //   "2001": artoo,
  //   ]

}

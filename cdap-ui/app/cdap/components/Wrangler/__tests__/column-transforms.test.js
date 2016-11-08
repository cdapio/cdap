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

import {
  renameColumn,
  dropColumn,
  splitColumn,
  mergeColumn
} from 'components/Wrangler/column-transforms';

describe('Wrangler: Column Transforms', () => {
  var array;

  beforeEach(() => {
    array = [
      {
        name: 'Thor Odinson',
        age: '200'
      },
      {
        name: 'Loki Odinson',
        age: '28'
      },
      {
        name: 'David Pumpkin',
        age: '500'
      },
      {
        name: 'Dracula Bloodsucker',
        age: '1500'
      },
      {
        name: 'Captain America',
        age: '500'
      }
    ];
  });

  it('should rename column', () => {
    const oldName = 'name';
    const newName = 'newName';

    let formattedData = renameColumn(array, oldName, newName);

    expect(formattedData[0][oldName]).toBeUndefined();
    expect(formattedData[0][newName]).toBe(array[0][oldName]);
    expect(formattedData[3][oldName]).toBeUndefined();
    expect(formattedData[3][newName]).toBe(array[3][oldName]);
  });

  it('should drop column', () => {
    const columnToDrop = 'age';

    let formattedData = dropColumn(array, columnToDrop);

    expect(formattedData[0][columnToDrop]).toBeUndefined();
    expect(formattedData[3][columnToDrop]).toBeUndefined();
  });

  it('should split column', () => {
    const columnToSplit = 'name';
    const firstSplit = 'firstname';
    const secondSplit = 'lastname';
    const delimiter = ' ';

    let formattedData = splitColumn(array, delimiter, columnToSplit, firstSplit, secondSplit);

    let split = array[0][columnToSplit].split(' ');

    expect(formattedData[0][firstSplit]).toBe(split[0]);
    expect(formattedData[0][secondSplit]).toBe(split[1]);
  });

  it('should merge column', () => {
    const joinKey = ' ';
    const firstColumn = 'name';
    const secondColumn = 'age';
    const mergedColumnName = 'merged';

    let formattedData = mergeColumn(array, joinKey, firstColumn, secondColumn, mergedColumnName);

    let merged = array[0][firstColumn] + joinKey + array[0][secondColumn];

    expect(formattedData[0][mergedColumnName]).toBe(merged);
  });
});

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
  mergeColumn,
  uppercaseColumn,
  lowercaseColumn,
  titlecaseColumn,
  substringColumn
} from 'wrangler/components/Wrangler/column-transforms';

describe('Wrangler: Column Transforms', () => {
  var array;

  beforeEach(() => {
    array = [
      {
        name: 'thor odinson',
        age: '200'
      },
      {
        name: 'loki odinson',
        age: '28'
      },
      {
        name: 'david pumpkin',
        age: '500'
      },
      {
        name: 'dracula bloodsucker',
        age: '1500'
      },
      {
        name: 'captain america',
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

  it('should uppercase column', () => {
    const columnToTransform = 'name';

    let formattedData = uppercaseColumn(array, columnToTransform);

    expect(formattedData[0][columnToTransform]).toBe(array[0][columnToTransform].toUpperCase());
    expect(formattedData[4][columnToTransform]).toBe(array[4][columnToTransform].toUpperCase());
  });

  it('should lowercase column', () => {
    const columnToTransform = 'name';

    let formattedData = lowercaseColumn(array, columnToTransform);

    expect(formattedData[0][columnToTransform]).toBe(array[0][columnToTransform].toLowerCase());
    expect(formattedData[4][columnToTransform]).toBe(array[4][columnToTransform].toLowerCase());
  });

  it('should titlecase column', () => {
    const columnToTransform = 'name';

    let formattedData = titlecaseColumn(array, columnToTransform);

    const case1Result = 'Thor Odinson';
    const case3Result = 'David Pumpkin';

    expect(formattedData[0][columnToTransform]).toBe(case1Result);
    expect(formattedData[2][columnToTransform]).toBe(case3Result);
  });

  it('should substring column', () => {
    const columnToSub = 'name';
    const begin = 0;
    const end = 5;
    const newColumn = 'substring';

    let formattedData = substringColumn(array, columnToSub, begin, end, newColumn);

    expect(formattedData[0][newColumn]).toBe(array[0][columnToSub].substr(begin, end));
    expect(formattedData[0][columnToSub]).toBe(array[0][columnToSub]);
    expect(formattedData[3][newColumn]).toBe(array[3][columnToSub].substr(begin, end));
    expect(formattedData[3][columnToSub]).toBe(array[3][columnToSub]);
  });
});

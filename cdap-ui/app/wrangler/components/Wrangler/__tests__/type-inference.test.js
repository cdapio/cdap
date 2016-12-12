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

import {inferType, inferColumn} from 'wrangler/components/Wrangler/type-inference';


describe('Wrangler: Type Inference', () => {
  it('should infer string', () => {
    const input1 = 'test 123';
    const input2 = '123 test';

    expect(inferType(input1)).toBe('string');
    expect(inferType(input2)).toBe('string');
  });

  it('should infer booleans', () => {
    const trueInput = 'true';
    const falseInput = 'false';

    expect(inferType(trueInput)).toBe('boolean');
    expect(inferType(falseInput)).toBe('boolean');
  });

  it('should infer integer', () => {
    const integerInput = '135';
    const wrongInteger = '08';

    expect(inferType(integerInput)).toBe('int');
    expect(inferType(wrongInteger)).not.toBe('int');
  });

  it('should infer float', () => {
    const floatInput = '1.35';
    const floatInput2 = '0.35';

    expect(inferType(floatInput)).toBe('float');
    expect(inferType(floatInput2)).toBe('float');
  });

  it('should throw error if input is not a string', () => {
    try {
      inferType(1);
    } catch (e) {
      expect(e).toContain('Input is not a string');
    }
  });
});

describe('Wrangler: Column Type Inference', () => {
  it('should infer integer column type', () => {
    const array = [
      {
        name: 'Thor',
        age: '200'
      },
      {
        name: 'Loki',
        age: '28'
      },
      {
        name: 'Quasimodo',
        age: '500'
      },
      {
        name: 'Dracula',
        age: '1500'
      },
      {
        name: 'Captain America',
        age: '500'
      }
    ];

    expect(inferColumn(array, 'age')).toBe('int');
  });

  it('should infer string when column type does not exceed threshold', () => {
    const array = [
      {
        name: 'Thor',
        age: '200'
      },
      {
        name: 'Loki',
        age: '28'
      },
      {
        name: 'Quasimodo',
        age: 'five hundred'
      },
      {
        name: 'Dracula',
        age: 'one thousand'
      },
      {
        name: 'Captain America',
        age: '1000 thousand five hundred'
      }
    ];

    expect(inferColumn(array, 'age')).toBe('string');
  });
});

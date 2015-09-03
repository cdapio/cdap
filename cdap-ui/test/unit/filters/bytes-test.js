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

'use strict';

describe('bytes', function() {

  beforeEach(module('cdap-ui.filters'));

  var bytes;

  beforeEach(inject(function(_$filter_) {
    bytes = _$filter_('bytes');
  }));


  it('should convert bytes to kB', function() {
    var b = 1024;

    expect(bytes(b)).toBe('1.0kB');
  });

  it('should convert bytes to MB', function() {
    var b = 1048576;

    expect(bytes(b)).toBe('1.0MB');
  });

  it('should convert bytes to GB', function() {
    var b = 1073741824;

    expect(bytes(b)).toBe('1.0GB');
  });

  it('should convert bytes to TB', function() {
    var b = 1099511627776;

    expect(bytes(b)).toBe('1.0TB');
  });

  it('should convert bytes to PB', function() {
    var b = 1099511627776 * 1024;

    expect(bytes(b)).toBe('1.0PB');
  });

  it('should return 0 bytes when input is not a number', function() {

    var input = 'str';
    expect(bytes(input)).toBe('0b');

    input = [1, 2, 3];
    expect(bytes(input)).toBe('0b');

    input = { 1: 1, 2: 2};
    expect(bytes(input)).toBe('0b');

    input = null;
    expect(bytes(input)).toBe('0b');

    input = undefined;
    expect(bytes(input)).toBe('0b');
  });

});
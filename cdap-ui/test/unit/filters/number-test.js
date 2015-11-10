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

describe('myNumber', function() {

  beforeEach(module('cdap-ui.filters'));

  var number;

  beforeEach(inject(function(_$filter_) {
    number = _$filter_('myNumber');

  }));

  it('should not convert number < 1000 ', function() {
    var b = 10;

    expect(number(b)).toBe('10');
  });

  it('should convert to k ', function() {
    var b = 1000;

    expect(number(b)).toBe('1k');
  });

  it('should convert to M ', function() {
    var b = 1000000;

    expect(number(b)).toBe('1.0M');
  });

  it('should convert to G ', function() {
    var b = 1000000000;

    expect(number(b)).toBe('1.0G');
  });

  it('should convert to T ', function() {
    var b = 1000000000000;

    expect(number(b)).toBe('1.0T');
  });

  it('should convert to P ', function() {
    var b = 1000000000000000;

    expect(number(b)).toBe('1.0P');
  });

});
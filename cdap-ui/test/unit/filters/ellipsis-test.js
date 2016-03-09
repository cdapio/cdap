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

describe('myEllipsis', function() {

  beforeEach(module('cdap-ui.filters'));

  var ellipsis;

  beforeEach(inject(function(_$filter_) {
    ellipsis = _$filter_('myEllipsis');

  }));


  it('should truncate string longer that limit to limit plus ellipsis', function() {

    var input = 'string longer than ten characters';
    var limit = 10;

    var result = ellipsis(input, limit);

    expect(result.length).toBe(limit + 1);
    expect(result).toBe(input.substr(0, limit-1) + '\u2026 ');

  });

  it('should not truncate string with length less than limit', function() {

    var input = 'short';
    var limit = 10;

    var result = ellipsis(input, limit);

    expect(result.length).toBe(input.length);
    expect(result).toEqual(input);

  });

  it('should return input when input is not type string', function() {

    var input = 12;
    expect(ellipsis(input, 10)).toEqual(input);

    input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    expect(ellipsis(input, 5)).toEqual(input);

    input = {
      1: 1,
      2: 2,
      3: 3,
      4: 4,
      5: 5
    };
    expect(ellipsis(input, 3)).toEqual(input);

  });

  it('should return null when input is null', function() {

    var input = null;
    expect(ellipsis(input, 10)).toEqual(null);

  });

});
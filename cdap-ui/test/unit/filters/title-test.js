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

describe('myTitleFilter', function() {

  beforeEach(module('cdap-ui.filters'));

  var title;

  beforeEach(inject(function(_$filter_) {
    title = _$filter_('myTitleFilter');
  }));

  it('should append CDAP to title', function() {
    var input = {
      data: {
        title: 'test'
      }
    };

    expect(title(input)).toBe(input.data.title + ' | CDAP');
  });

  it('should return CDAP when title is not present', function() {
    var input = {};

    expect(title(input)).toBe('CDAP');
  });

});
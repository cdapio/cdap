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


describe('test env', function(){

  it('is ready', function() {
    expect(jasmine).toBeDefined();
    expect(angular).toBeDefined();
    expect(module).toBe(angular.mock.module);
    expect(inject).toBe(angular.mock.inject);
  });

});


describe('directive tests', function() {

  it('moved to *-unit-test.js files inside each directive directory', function() {
    expect(true).toBeTruthy();
  });

});


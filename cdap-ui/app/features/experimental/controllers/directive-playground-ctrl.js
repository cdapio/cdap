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

angular.module(`${PKG.name}.feature.experimental`)
  .controller('DirectivePlayGround', function() {
    this.dsMultipleValuesModel = 'value1:value2:value3,newValue1:newValue2:newValue3';

    this.topAppsData = {
      'total' : 15,
      'results' : [
        {
          'label' : 'Application1',
          'value' : 93
        },
        {
          'label' : 'Application3',
          'value' : 61
        },
        {
          'label' : 'Application2',
          'value' : 32
        },
        {
          'label' : 'Application5',
          'value' : 32
        },
        {
          'label' : 'Application4',
          'value' : 1
        }
      ]
    };
  });

/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

angular.module(PKG.name + '.commons')
  .factory('MyPipelineResourceFactory', function() {
    let attributes = {
      'store': 'store',
      'action-creator': 'actionCreator',
      'is-disabled': 'isDisabled'
    };
    return {
      'driverResource': {
        'element': '<my-pipeline-driver-resource></my-pipeline-driver-resource>',
        'attributes': attributes
      },
      'clientResource': {
        'element': '<my-pipeline-client-resource></my-pipeline-client-resource>',
        'attributes': attributes
      },
      'executorResource': {
        'element': '<my-pipeline-executor-resource></my-pipeline-executor-resource>',
        'attributes': attributes
      }
    };
  });

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

angular.module(PKG.name + '.feature.streams')
  .controller('StreamDetailStatusController', function($scope, $state, myHelpers, MyCDAPDataSource, rStreamData) {
    var dataSrc = new MyCDAPDataSource($scope);
    this.storage = null;
    this.events = null;

    this.schema = rStreamData.format.schema.fields;

    [
      {
        name: 'system.collect.bytes',
        scopeProperty: 'storage'
      },
      {
        name: 'system.collect.events',
        scopeProperty: 'events'
      }
    ].forEach(fetchMetric.bind(this));

    function fetchMetric(metric) {
      var path = '/metrics/query?metric=' + metric.name +
                  '&tag=ns:' + $state.params.namespace +
                  '&tag=stream:' + $state.params.streamId;

      dataSrc.poll({
        _cdapPath : path ,
        method: 'POST'
      }, function(metricData) {
          var data = myHelpers.objectQuery(metricData, 'series', 0, 'data', 0, 'value');
          this[metric.scopeProperty] = data;

          /**
          * FIX ME: JavaScript largest possible number: 9007199254740992
          * The backend stores number as 64 bit long. Max: 9,223,372,036,854,775,807
          * JavaScript is missing 3 digits. Beyond the JS number, it will round-of the value
          **/

      }.bind(this));
    }
  });

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

    this.topAppsData2 = {
      'total' : 2,
      'results' : [
        {
          'label' : 'Application1',
          'value' : 85
        },
        {
          'label' : 'Application3',
          'value' : 61
        }
      ]
    };


    this.auditLogCounts = {
      'bucket_interval' : 'day',
      'results' : [
        {
          'timestamp' : 1451606400,
          'log_count' : 25
        },
        {
          'timestamp' : 1451692800,
          'log_count' : 26
        },
        {
          'timestamp' : 1451779200,
          'log_count' : 27
        },
        {
          'timestamp' : 1451865600,
          'log_count' : 22
        },
        {
          'timestamp' : 1451952000,
          'log_count' : 29
        },
        {
          'timestamp' : 1452038400,
          'log_count' : 0
        },
        {
          'timestamp' : 1452124800,
          'log_count' : 32
        },
        {
          'timestamp' : 1452211200,
          'log_count' : 33
        },
        {
          'timestamp' : 1452297600,
          'log_count' : 1
        },
        {
          'timestamp' : 1452384000,
          'log_count' : 4
        },
        {
          'timestamp' : 1452470400,
          'log_count' : 4
        },
        {
          'timestamp' : 1452556800,
          'log_count' : 4
        },
        {
          'timestamp' : 1452643200,
          'log_count' : 4
        },
        {
          'timestamp' : 1452729600,
          'log_count' : 4
        },
        {
          'timestamp' : 1452816000,
          'log_count' : 4
        },
        {
          'timestamp' : 1452902400,
          'log_count' : 4
        },
        {
          'timestamp' : 1452988800,
          'log_count' : 4
        },
        {
          'timestamp' : 1453075200,
          'log_count' : 4
        },
        {
          'timestamp' : 1453161600,
          'log_count' : 4
        },
        {
          'timestamp' : 1453248000,
          'log_count' : 0
        },
        {
          'timestamp' : 1453334400,
          'log_count' : 0
        },
        {
          'timestamp' : 1453420800,
          'log_count' : 5
        },
        {
          'timestamp' : 1453507200,
          'log_count' : 7
        },
        {
          'timestamp' : 1453593600,
          'log_count' : 12
        },
        {
          'timestamp' : 1453680000,
          'log_count' : 4
        },
        {
          'timestamp' : 1453766400,
          'log_count' : 2
        },
        {
          'timestamp' : 1453852800,
          'log_count' : 1
        },
        {
          'timestamp' : 1453939200,
          'log_count' : 4
        },
        {
          'timestamp' : 1454025600,
          'log_count' : 4
        },
        {
          'timestamp' : 1454112000,
          'log_count' : 4
        },
        {
          'timestamp' : 1454198400,
          'log_count' : 7
        },
        {
          'timestamp' : 1454284800,
          'log_count' : 4
        },
        {
          'timestamp' : 1454371200,
          'log_count' : 4
        },
        {
          'timestamp' : 1454457600,
          'log_count' : 9
        },
        {
          'timestamp' : 1454544000,
          'log_count' : 7
        }
      ]
    };

  });

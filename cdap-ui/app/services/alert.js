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

angular.module(PKG.name+'.services')
  .service('myAlert', function(){
    var __list = [];
    function alert(item) {
      if (angular.isObject(item) && Object.keys(item).length) {
        if (__list.length > 0 && __list[0].content === item.content && __list[0].title === item.title) {
          __list[0].count++;
          __list[0].time = Date.now();

        } else {
          __list.unshift({
            content: item.content,
            title: item.title,
            time: Date.now(),
            count: 1
          });
        }
      }
    }

    alert['clear'] = function() {
      __list = [];
    };

    alert['isEmpty'] = function() {
      return __list.length === 0;
    };

    alert['getAlerts'] = function() {
      return __list;
    };

    alert['count'] = function() {
      return __list.length;
    };

    alert['remove'] = function(item) {
      __list.splice(__list.indexOf(item), 1);
    };

    return alert;
  });

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

angular.module(PKG.name + '.feature.foo')
  .controller('FooPlaygroundController',
    function ($scope, $q, mySessionStorage, myLocalStorage, mySettings) {

      // meta to the max!

      var solutions = [
        { n:'Session',  v:mySessionStorage },
        { n:'Local',    v:myLocalStorage },
        { n:'Settings', v:mySettings }
      ];

      /*
        naming scheme for preferences:

        first component: "feature" because it's not a "service" preference
        second component: "foo" feature name
        last component(s): state the pref originate from
       */
      var PREF_KEY = 'feature.foo.test-settings';

      $q.all(solutions.map(function(s){
        return s.v.get(PREF_KEY);
      }))
      .then(function(result){
        var s;
        for (var i = 0; i < result.length; i++) {
          s = solutions[i];
          $scope[s.n.toLowerCase()] = result[i];
          $scope['doSave'+s.n] = (function() {
            this.v.set(PREF_KEY, $scope[this.n.toLowerCase()]);
          }).bind(s);
        }
      });


    });

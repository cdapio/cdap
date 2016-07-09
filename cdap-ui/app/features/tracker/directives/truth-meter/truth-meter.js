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

// function truthMeterController ($scope) {
//   'ngInject';

//   this.truthScore = $scope.truthScore;
//   console.log('this.truthScore: ', $scope.truthScore);
// }

angular.module(PKG.name + '.feature.tracker')
  .directive('myTruthMeter', function () {

    return {
      restrict: 'E',
      scope: {
        model: '=',
        score: '=',
        multiplier: '=',
        width: '@'
      },
      templateUrl: '/assets/features/tracker/directives/truth-meter/truth-meter.html',
      controller: function($scope) {
        this.score = $scope.score;
        this.multiplier = $scope.multiplier;
        this.width = $scope.width;
      },
      controllerAs: 'TruthMeter'
    };
  });

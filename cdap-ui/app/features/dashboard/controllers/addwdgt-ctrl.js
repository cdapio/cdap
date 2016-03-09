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

/**
 * DashboardAddWdgtCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardAddWdgtCtrl',
function ($scope, $modalInstance, caskFocusManager, Widget) {

  caskFocusManager.focus('addWdgtType');

  $scope.model = new Widget();
  $scope.addType = 'INDIVIDUAL';

  $scope.widgetTypes = [
    { name: 'Line',                  type: 'c3-line'},
    { name: 'Bar',                   type: 'c3-bar'},
    { name: 'Pie',                   type: 'c3-pie'},
    { name: 'Donut',                 type: 'c3-donut'},
    { name: 'Scatter',               type: 'c3-scatter'},
    { name: 'Spline',                type: 'c3-spline'},
    { name: 'Area',                  type: 'c3-area'},
    { name: 'Area Spline',           type: 'c3-area-spline'},
    { name: 'Area Spline Stacked',   type: 'c3-area-spline-stacked'},
    { name: 'Area Step',             type: 'c3-area-step'},
    { name: 'Step',                  type: 'c3-step'},
    { name: 'Table',                 type: 'table' }
  ];

  $scope.addWidget = function () {
    if ($scope.addType === 'MULTIPLE') {
      $scope.addMetricsToWidget();
    } else {
      $scope.addMetricsToIndividualWidgets();
    }
  };


  $scope.addMetricsToIndividualWidgets = _.debounce(function() {
    var widgets = [];
    angular.forEach($scope.model.metric.names, function(value) {
      widgets.push(
        new Widget({
          type: $scope.model.type,
          title: value,
          metric: {
            context: $scope.model.metric.context,
            names: [value],
            name: value
          }
        })
      );
    });
    $scope.currentBoard.addWidget(widgets);
    $scope.$close();
  }, 1000);

  $scope.addMetricsToWidget = _.debounce(function () {
    var metrics = $scope.model.metric;
    if (!metrics) {
      return;
    }
    $scope.currentBoard.addWidget($scope.model);
    $scope.$close();
  }, 1000);

  $scope.closeModal = function() {
    $modalInstance.close();
  };

});

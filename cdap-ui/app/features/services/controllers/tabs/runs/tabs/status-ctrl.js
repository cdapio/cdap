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

angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunsDetailStatusController', function($state, $scope, myServiceApi, $uibModal) {

    var path = '/apps/' +
          $state.params.appId + '/services/' +
          $state.params.programId;

    this.endPoints = [];

    this.basePath = '/namespaces/' + $state.params.namespace + path;

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      serviceId: $state.params.programId,
      scope: $scope
    };

    myServiceApi.get(params)
      .$promise
      .then(function(res) {
        angular.forEach(res.handlers, function(value) {
          this.endPoints = this.endPoints.concat(value.endpoints);
        }, this);
      }.bind(this));

    if ($scope.RunsController.runs.length > 0) {
      params.runId = $scope.RunsController.runs.selected.runid;

      myServiceApi.runDetail(params)
        .$promise
        .then(function(res) {
          this.status = res.status;
        }.bind(this));
    }

    this.openModal = function (endpoint) {
      $uibModal.open({
        templateUrl: '/assets/features/services/templates/tabs/runs/tabs/status/make-request.html',
        keyboard: true,
        backdrop: 'static',
        size: 'lg',
        controller: 'StatusMakeRequestController',
        controllerAs: 'RequestController',
        resolve: {
          rRequestUrl: function () {
            return endpoint.path;
          },
          rRequestMethod: function () {
            return endpoint.method;
          }
        }
      });
    };

  });

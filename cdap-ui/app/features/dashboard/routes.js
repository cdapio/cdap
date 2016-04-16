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

angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('dashboard', {
        url: '/dashboard',
        parent: 'ns',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'dashboard'
        },
        templateUrl: '/assets/features/dashboard/templates/dashboard.html',
        resolve: {
          rDashboardsModel: function ($stateParams, MyDashboardsModel) {
            return (new MyDashboardsModel($stateParams.namespace)).$promise;
          }
        },
        controller: 'DashboardCtrl'
      })

      .state('dashboard.standard', {
        url: '/standard',
        abstract: true,
        template: '<ui-view/>'
      })

      .state('dashboard.standard.cdap', {
        url: '/cdap',
        templateUrl: '/assets/features/dashboard/templates/staticdashboard.html',
        controller: 'OpsCdapCtrl'
      })

      .state('dashboard.user', {
        url: '/user/:tab',
        templateUrl: '/assets/features/dashboard/templates/userdashboard.html',
        controller: 'UserDashboardCtrl',
        resolve: {
          tab: function($stateParams) {
            return $stateParams.tab;
          }
        }
      })

      .state('dashboard.user.addwdgt', {
        url: '/widget/add',
        onEnter: function ($stateParams, $state, $uibModal, $rootScope, rDashboardsModel, tab) {
          var scope = $rootScope.$new();
          var currentBoard = rDashboardsModel.data[$stateParams.tab];
          scope.currentBoard = currentBoard;
          scope.metricsLimit = currentBoard.WIDGET_LIMIT;
          scope.metricsSlotsFilled = currentBoard.columns.length;

          $uibModal.open({
            templateUrl: '/assets/features/dashboard/templates/partials/addwdgt.html',
            size: 'lg',
            backdrop: true,
            windowClass: 'cdap-modal',
            keyboard: true,
            scope: scope,
            controller: 'DashboardAddWdgtCtrl'
          }).result.finally(function() {
            $state.go('dashboard.user', {tab: tab}, { reload: true });
          });
        },
        onExit: function($uibModalStack) {
          $uibModalStack.dismissAll();
        }
      })

      ;


  });

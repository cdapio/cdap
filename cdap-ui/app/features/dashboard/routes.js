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
        controller: 'DashboardCtrl',
        ncyBreadcrumb: {
          skip: true
        }
      })

      .state('dashboard.standard', {
        url: '/standard',
        abstract: true,
        template: '<ui-view/>'
      })

      .state('dashboard.standard.cdap', {
        url: '/cdap',
        templateUrl: '/assets/features/dashboard/templates/staticdashboard.html',
        controller: 'OpsCdapCtrl',
        ncyBreadcrumb: {
          label: 'Operations'
        }
      })

      .state('dashboard.user', {
        url: '/user/:tab',
        templateUrl: '/assets/features/dashboard/templates/userdashboard.html',
        controller: 'UserDashboardCtrl',
        resolve: {
          tab: function($stateParams) {
            return $stateParams.tab;
          }
        },
        ncyBreadcrumb: {
          label: 'Operations'
        }
      })

      .state('dashboard.user.addwdgt', {
        url: '/widget/add',
        onEnter: function ($state, $bootstrapModal, $rootScope, rDashboardsModel, tab) {
          var scope = $rootScope.$new();
          scope.currentBoard = rDashboardsModel.current();
          $bootstrapModal.open({
            templateUrl: '/assets/features/dashboard/templates/partials/addwdgt.html',
            size: 'md',
            backdrop: true,
            keyboard: true,
            scope: scope,
            controller: 'DashboardAddWdgtCtrl'
          }).result.finally(function() {
            $state.go('dashboard.user', {tab: tab}, { reload: true });
          });
        },
        onExit: function($modalStack) {
          $modalStack.dismissAll();
        }
      })

      ;


  });

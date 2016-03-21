/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

angular.module(PKG.name + '.feature.hydrator-beta')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('hydrator-beta', {
        url: '/hydrator-beta',
        abstract: true,
        parent: 'ns',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'hydratorList'
        },
        template: '<ui-view/>'
      })

        .state('hydrator-beta.create', {
          url: '/studio?draftId',
          params: {
            data: null
          },
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorStudioBeta'
          },
          resolve: {
            rConfig: function($stateParams, mySettings, $q, myHelpers) {
              var defer = $q.defer();
              if ($stateParams.draftId) {
                mySettings.get('hydratorDrafts', true)
                  .then(function(res) {
                    var draft = myHelpers.objectQuery(res, $stateParams.namespace, $stateParams.draftId);
                    if (angular.isObject(draft)) {
                      defer.resolve(draft);
                    } else {
                      defer.resolve(false);
                    }
                  });
              } else if ($stateParams.data){
                defer.resolve($stateParams.data);
              } else {
                defer.resolve(false);
              }
              return defer.promise;
            },
            rVersion: function($state, MyCDAPDataSource) {
              var dataSource = new MyCDAPDataSource();
              return dataSource.request({
                _cdapPath: '/version'
              });
            }
          },
          views: {
            '': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/studio.html',
              controller: 'HydratorPlusPlusStudioCtrl as HydratorPlusPlusStudioCtrl'
            },
            'canvas@hydrator-beta.create': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/canvas.html',
              controller: 'HydratorPlusPlusCreateCanvasCtrl',
              controllerAs: 'CanvasCtrl'
            },
            'leftpanel@hydrator-beta.create': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/leftpanel.html',
              controller: 'HydratorPlusPlusLeftPanelCtrl as HydratorPlusPlusLeftPanelCtrl'
            },
            'toppanel@hydrator-beta.create': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/toppanel.html',
              controller: 'HydratorPlusPlusTopPanelCtrl as HydratorPlusPlusTopPanelCtrl'
            },
            'bottompanel@hydrator-beta.create': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/bottompanel.html',
              controller: 'HydratorPlusPlusBottomPanelCtrl as HydratorPlusPlusBottomPanelCtrl'
            }
          },
          onExit: function($uibModalStack) {
            $uibModalStack.dismissAll();
          }
        });

  });

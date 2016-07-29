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

angular.module(PKG.name + '.commons')
  .controller('MyRealtimePipelineSettingsCtrl', function($scope, MY_CONFIG) {
    this.instance = this.store.getInstance();
    this._isDisabled = this.isDisabled === 'true';
    this.isDistributed = MY_CONFIG.isEnterprise ? true : false;
    if (!this.isDistributed) {
      this.isStandaloneMessage = 'Resources don\'t take any effect in standalone environment.';
    }
    this.memoryMb = this.store.getMemoryMb();
    this.virtualCores = this.store.getVirtualCores();
    if (!this._isDisabled) {
      // Debounce method for setting instance
      const setInstance = _.debounce( () => {
        this.actionCreator.setInstance(this.instance);
      }, 1000);
      var unsub = $scope.$watch('MyRealtimePipelineSettingsCtrl.instance' , setInstance);
      $scope.$on('$destroy', unsub);
    }
    this.onMemoryMbChange = () => {
      this.actionCreator.setMemoryMb(this.memoryMb);
    };
    this.onVirtualCoresChange = () => {
      this.actionCreator.setVirtualCores(this.virtualCores);
    };
  });

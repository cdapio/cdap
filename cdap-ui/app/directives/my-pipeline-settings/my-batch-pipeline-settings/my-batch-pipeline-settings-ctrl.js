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

class MyBatchPipelineSettingsCtrl {
  constructor(GLOBALS, $scope, MY_CONFIG) {
    this.GLOBALS = GLOBALS;
    this.isDistributed = MY_CONFIG.isEnterprise ? true : false;
    this._isDisabled = this.isDisabled === 'true';
    this.templateType = this.store.getArtifact().name;
    this.scheduleWidget = {
      type: 'basic'
    };
    this.initialCron = this.store.getSchedule();
    this.cron = this.initialCron;
    if (!this.checkCron(this.initialCron)) {
      this.scheduleWidget.type = 'advanced';
    }
    this.engine = this.store.getEngine();
    if (!this._isDisabled) {
      // Debounce method for setting schedule
      var setSchedule = _.debounce(() => this.actionCreator.setSchedule(this.cron), 1000);
      var unsub = $scope.$watch('MyBatchPipelineSettingsCtrl.cron', setSchedule);
      $scope.$on('$destroy', unsub);
    }
  }
  checkCron(cron) {
    var pattern = /^[0-9\*\s]*$/g;
    var parse = cron.split('');
    for (var i = 0; i < parse.length; i++) {
      if (!parse[i].match(pattern)) {
        return false;
      }
    }
    return true;
  }

  onEngineChange() {
    this.actionCreator.setEngine(this.engine);
  }
  changeScheduler () {
    this.initialCron = this.store.getDefaultSchedule();
    this.cron = this.initialCron;
  }
}

MyBatchPipelineSettingsCtrl.$inject = ['GLOBALS', '$scope', 'MY_CONFIG'];
angular.module(PKG.name + '.commons')
  .controller('MyBatchPipelineSettingsCtrl', MyBatchPipelineSettingsCtrl);

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

class HydratorPlusPlusSettingsCtrl {
  constructor(GLOBALS, HydratorPlusPlusConfigStore, HydratorPlusPlusConfigActions, $scope) {
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.templateType = HydratorPlusPlusConfigStore.getArtifact().name;

    this.activeTab = 0;

    // If ETL Batch
    if (GLOBALS.etlBatchPipelines.indexOf(this.templateType) !== -1) {
      // Initialiting ETL Batch Schedule
      this.initialCron = HydratorPlusPlusConfigStore.getSchedule();

      this.cron = this.initialCron;
      this.engine = HydratorPlusPlusConfigStore.getEngine();
      this.isBasic = this.checkCron(this.initialCron);

      this.activeTab = this.isBasic ? 0 : 1;

      // Debounce method for setting schedule
      var setSchedule = _.debounce( () => {
        HydratorPlusPlusConfigActions.setSchedule(this.cron);
      }, 1000);

      $scope.$watch( () => {
        return this.cron;
      }, setSchedule);
    }
    // If ETL Realtime
    else if (this.templateType === GLOBALS.etlRealtime) {
      // Initializing ETL Realtime Instance
      this.instance = HydratorPlusPlusConfigStore.getInstance();

      // Debounce method for setting instance
      var setInstance = _.debounce( () => {
        HydratorPlusPlusConfigActions.setInstance(this.instance);
      }, 1000);


      $scope.$watch( () => {
        return this.instance;
      }, setInstance);
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
    this.HydratorPlusPlusConfigActions.setEngine(this.engine);
  }
  changeScheduler (type) {
    if (type === 'BASIC') {
      this.activeTab = 0;
      this.initialCron = this.cron;
      var check = true;
      if (!this.checkCron(this.initialCron)) {
        check = confirm('You have advanced configuration that is not available in basic mode. Are you sure you want to go to basic scheduler?');
      }
      if (check) {
        this.isBasic = true;
      }
    } else {
      this.activeTab = 1;
      this.isBasic = false;
    }
  }
}

HydratorPlusPlusSettingsCtrl.$inject = ['GLOBALS', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusConfigActions', '$scope'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusSettingsCtrl', HydratorPlusPlusSettingsCtrl);

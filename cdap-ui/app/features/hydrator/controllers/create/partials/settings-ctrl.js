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

class HydratorSettingsController {
  constructor(GLOBALS, ConfigStore, ConfigActionsFactory, $scope) {
    this.GLOBALS = GLOBALS;

    this.templateType = ConfigStore.getArtifact().name;

    // If ETL Batch
    if (this.templateType === GLOBALS.etlBatch) {
      // Initialiting ETL Batch Schedule
      this.initialCron = ConfigStore.getSchedule();
      this.cron = this.initialCron;

      this.isBasic = this.checkCron(this.initialCron);


      // Debounce method for setting schedule
      var setSchedule = _.debounce( () => {
        ConfigActionsFactory.setSchedule(this.cron);
      }, 1000);

      $scope.$watch( () => {
        return this.cron;
      }, setSchedule);
    }
    // If ETL Realtime
    else if (this.templateType === GLOBALS.etlRealtime) {
      // Initializing ETL Realtime Instance
      this.instance = ConfigStore.getInstance();

      // Debounce method for setting instance
      var setInstance = _.debounce( () => {
        ConfigActionsFactory.setInstance(this.instance);
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

  changeScheduler (type) {
    if (type === 'BASIC') {
      this.initialCron = this.cron;
      var check = true;
      if (!this.checkCron(this.initialCron)) {
        check = confirm('You have advanced configuration that is not available in basic mode. Are you sure you want to go to basic scheduler?');
      }
      if (check) {
        this.isBasic = true;
      }
    } else {
      this.isBasic = false;
    }
  }
}

HydratorSettingsController.$inject = ['GLOBALS', 'ConfigStore', 'ConfigActionsFactory', '$scope'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorSettingsController', HydratorSettingsController);
  // .controller('HydratorSettingsController', function() {
    //GLOBALS, EventPipe, $timeout, myHelpers, $scope
    // this.GLOBALS = GLOBALS;
    // this.metadata = MyAppDAGService.metadata;
    // this.initialCron = MyAppDAGService.metadata.template.schedule.cron || '* * * * *';
    //
    // this.cron = myHelpers.objectQuery(MyAppDAGService, 'metadata', 'template', 'schedule', 'cron') || '';
    // this.instance = myHelpers.objectQuery(MyAppDAGService, 'metadata', 'template', 'instance');
    // this.saveDisabled = true;
    //
    // if (MyAppDAGService.metadata.template.type === GLOBALS.etlBatch) {
    //   $scope.$watch(function () {
    //     return this.cron;
    //   }.bind(this), function () {
    //     var initialCron = myHelpers.objectQuery(MyAppDAGService, 'metadata', 'template', 'schedule', 'cron') || '';
    //     if (this.cron !== initialCron) {
    //       this.saveDisabled = false;
    //     } else {
    //       this.saveDisabled = true;
    //     }
    //   }.bind(this));
    // }
    //
    // function checkCron(cron) {
    //   var pattern = /^[0-9\*\s]*$/g;
    //   var parse = cron.split('');
    //   for (var i = 0; i < parse.length; i++) {
    //     if (!parse[i].match(pattern)) {
    //       return false;
    //     }
    //   }
    //   return true;
    // }
    //
    // this.isBasic = checkCron(this.initialCron);
    //
    // this.changeScheduler = function (type) {
    //   if (type === 'BASIC') {
    //     this.initialCron = this.cron;
    //     var check = true;
    //     if (!checkCron(this.initialCron)) {
    //       check = confirm('You have advanced configuration that is not available in basic mode. Are you sure you want to go to basic scheduler?');
    //     }
    //     if (check) {
    //       this.isBasic = true;
    //     }
    //   } else {
    //     this.isBasic = false;
    //   }
    // };
    //
    // this.save = function () {
    //   this.saveDisabled = true;
    //   MyAppDAGService.metadata.template.schedule.cron = this.cron;
    //   MyAppDAGService.metadata.template.instance = this.instance;
    // };
    //
    // // Will be used once we figure out how to reset a bottom panel tab content.
    // this.reset = function() {
    //   $timeout(function () {
    //     this.initialCron = myHelpers.objectQuery(MyAppDAGService, 'metadata', 'template', 'schedule', 'cron') || '* * * * *';
    //   }.bind(this));
    //
    //   this.saveDisabled = true;
    //   this.instance = myHelpers.objectQuery(MyAppDAGService, 'metadata', 'template', 'instance');
    //
    //   EventPipe.emit('plugin.reset');
    // };

  // });

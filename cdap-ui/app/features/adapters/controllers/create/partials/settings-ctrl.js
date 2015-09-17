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

angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterSettingsController', function(MyAppDAGService, GLOBALS, EventPipe) {
    this.GLOBALS = GLOBALS;
    this.metadata = MyAppDAGService.metadata;
    var metadataCopy = angular.copy(MyAppDAGService.metadata);
    this.initialCron = this.metadata.template.schedule.cron;



    function checkCron(cron) {
      var pattern = /^[0-9\*\s]*$/g;
      var parse = cron.split('');
      for (var i = 0; i < parse.length; i++) {
        if (!parse[i].match(pattern)) {
          return false;
        }
      }
      return true;
    }

    this.isBasic = checkCron(this.initialCron);

    this.changeScheduler = function (type) {
      if (type === 'BASIC') {
        this.initialCron = this.metadata.template.schedule.cron;
        var check = true;
        if (!checkCron(this.initialCron)) {
          check = confirm('You have advanced configuration that is not available in basic mode. Are you sure you want to go to basic scheduler?');
        }
        if (check) {
          this.isBasic = true;
        }
      } else {
        this.isBasic = false;
      }
    };

    // Will be used once we figure out how to reset a bottom panel tab content.
    this.reset = function() {
      this.metadata.template.schedule.cron = metadataCopy.template.schedule.cron;
      this.initialCron = metadataCopy.template.schedule.cron;
      this.metadata.template.instance = metadataCopy.template.instance;
      EventPipe.emit('plugin.reset');
    };


  });

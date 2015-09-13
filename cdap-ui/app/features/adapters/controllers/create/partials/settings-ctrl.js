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
    // Will be used once we figure out how to reset a bottom panel tab content.
    this.reset = function() {
      this.metadata.template.schedule.cron = metadataCopy.template.schedule.cron;
      this.metadata.template.instance = metadataCopy.template.instance;
      EventPipe.emit('plugin.reset');
    };
  });

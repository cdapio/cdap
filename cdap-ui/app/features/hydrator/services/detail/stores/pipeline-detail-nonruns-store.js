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
 
angular.module(PKG.name + '.feature.hydrator')
  .service('DetailNonRunsStore', function(PipelineDetailDispatcher) {
    this.setDefaults = function() {
      this.state = {
        scheduleStatus: ''
      };
    };
    this.changeListeners = [];
    var dispatcher = PipelineDetailDispatcher.getDispatcher();

    this.setDefaults();
    this.getScheduleStatus = function() {
      return this.state.scheduleStatus;
    };

    this.registerOnChangeListener = function(callback) {
      this.changeListeners.push(callback);
    };
    this.emitChange = function() {
      this.changeListeners.forEach(function(callback) {
        callback(this.state);
      });
    };
    this.setState = function(schedule) {
      this.state.scheduleStatus = schedule.status;
      this.emitChange();
    };
    dispatcher.register('onScheduleStatusFetch', this.setState.bind(this));
  });

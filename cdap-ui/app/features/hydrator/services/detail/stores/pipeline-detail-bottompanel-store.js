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
  .service('BottomPanelStore', function(PipelineDetailBottomPanelDispatcher) {

    var dispatcher = PipelineDetailBottomPanelDispatcher.getDispatcher();
    this.changeListeners = [];
    this.setDefaults = function() {
      this.state = {
        panelState: 1
      };
    };
    this.setDefaults();

    this.getPanelState = function() {
      return this.state.panelState;
    };

    this.setState = function(panel) {
      this.state.panelState = panel.state;
      this.emitChange();
    };

    this.registerOnChangeListener = function(callback) {
      this.changeListeners.push(callback);
    };

    this.emitChange = function() {
      this.changeListeners.forEach(function(callback) {
        callback(this.state);
      }.bind(this));
    };
    dispatcher.register('onBottomPanelToggled', this.setState.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this));
  });

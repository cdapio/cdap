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

class PluginTemplateActionBeta {
  constructor(PluginTemplateDispatcherBeta) {
    this.PluginTemplateDispatcherBeta = PluginTemplateDispatcherBeta.getDispatcher();
  }

  init(state) {
    this.PluginTemplateDispatcherBeta.dispatch('onInit', state);
  }

  reset() {
    this.PluginTemplateDispatcherBeta.dispatch('onReset');
  }

  triggerSave() {
    this.PluginTemplateDispatcherBeta.dispatch('onSaveTriggered');
  }

  cancelTriggerSave() {
    this.PluginTemplateDispatcherBeta.dispatch('onSaveTriggerCancel');
  }

  templateSavedSuccesfully() {
    this.PluginTemplateDispatcherBeta.dispatch('onSaveSuccessfull');
  }
  templateClose(isCloseCommand) {
    this.PluginTemplateDispatcherBeta.dispatch('onCloseCommand', isCloseCommand);
  }

  templateSaveFailed(err) {
    this.PluginTemplateDispatcherBeta.dispatch('onSaveFailure', err);
  }

}
PluginTemplateActionBeta.$inject = ['PluginTemplateDispatcherBeta'];
angular.module(`${PKG.name}.commons`)
  .service('PluginTemplateActionBeta', PluginTemplateActionBeta);

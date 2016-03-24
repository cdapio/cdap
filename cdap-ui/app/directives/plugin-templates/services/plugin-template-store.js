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

class PluginTemplatesDirStore {
  constructor(PluginTemplatesDirDispatcher) {
    this.state = {};
    this.setDefaults();
    this.changeListeners = [];

    let dispatcher = PluginTemplatesDirDispatcher.getDispatcher();
    dispatcher.register('onInit', this.initStore.bind(this));
    dispatcher.register('onSaveSuccessfull', this.setIsSaveSuccessfull.bind(this));
    dispatcher.register('onCloseCommand', this.setIsCloseCommand.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this));
  }

  setDefaults() {
    this.state = {
      templateType: null,
      pluginType: null,
      pluginName: null,
      mode: null,
      isSaveSuccessful: null
    };
    this.changeListeners = [];
  }

  initStore(state) {
    this.state.pluginType = state.pluginType;
    this.state.templateType = state.templateType;
    this.state.templateName = state.templateName;
    this.state.pluginName = state.pluginName;
    this.state.mode = state.mode;
    this.emitChange();
  }

  getPluginType() {return this.state.pluginType;}
  getTemplateType() {return this.state.templateType;}
  getPluginName() {return this.state.pluginName;}
  setPluginName(pluginName) {this.state.pluginName = pluginName;}
  getTemplateName() {return this.state.templateName;}
  setTemplateName(pluginName) { this.state.templateName = pluginName; }
  getMode() {return this.state.mode;}
  setMode(mode) {this.state.mode = mode;}
  getIsSaveSuccessfull() {return this.state.isSaveSuccessful;}
  setIsSaveSuccessfull() {
    this.state.isSaveSuccessful = true;
    this.emitChange();
  }
  getIsCloseCommand() { return this.state.isCloseCommand;}
  setIsCloseCommand(isCloseCommand) {
    this.state.isCloseCommand = isCloseCommand;
    this.emitChange();
  }
  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }
}

PluginTemplatesDirStore.$inject = ['PluginTemplatesDirDispatcher'];

angular.module(`${PKG.name}.commons`)
  .service('PluginTemplatesDirStore', PluginTemplatesDirStore);

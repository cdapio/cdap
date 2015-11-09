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

class LeftPanelStore {
  constructor(LeftPanelDispatcher, PluginsDispatcher, MyDAGFactory) {
    this.state = {};
    this.setDefaults();
    this.MyDAGFactory = MyDAGFactory;
    this.changeListeners = [];
    let dispatcher = LeftPanelDispatcher.getDispatcher();
    dispatcher.register('onLeftPanelToggled', this.setState.bind(this));
    dispatcher.register('toggleLeftPanelState', this.togglePanelState.bind(this));

    let pluginsDispatcher = PluginsDispatcher.getDispatcher();
    pluginsDispatcher.register('onSourcesFetch', this.setSources.bind(this));
    pluginsDispatcher.register('onTransformsFetch', this.setTransforms.bind(this));
    pluginsDispatcher.register('onSinksFetch', this.setSinks.bind(this));
  }
  setDefaults() {
    this.state = {
      panelState: true,
      plugins: {}
    };
  }

  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }

  getState() {
    return this.state.panelState;
  }
  setState(state) {
    this.state = state;
    this.emitChange();
  }
  togglePanelState() {
    this.state.panelState = !this.state.panelState;
    this.emitChange();
  }

  setSources(plugins, type) {
    this.state.plugins.sources = plugins.map( plugin => {
      plugin.type = type;
      plugin.icon = this.MyDAGFactory.getIcon(plugin.name);
      return plugin;
    });
    this.emitChange();
  }
  getSources() {
    return this.state.plugins.sources;
  }

  setTransforms(plugins, type) {
    this.state.plugins.transforms = plugins.map( plugin => {
      plugin.type = type;
      plugin.icon = this.MyDAGFactory.getIcon(plugin.name);
      return plugin;
    });
    this.emitChange();
  }
  getTransforms() {
    return this.state.plugins.transforms;
  }

  setSinks(plugins, type) {
    this.state.plugins.sinks = plugins.map( plugin => {
      plugin.type = type;
      plugin.icon = this.MyDAGFactory.getIcon(plugin.name);
      return plugin;
    });
    this.emitChange();
  }
  getSinks() {
    return this.state.plugins.sinks;
  }
}

LeftPanelStore.$inject = ['LeftPanelDispatcher', 'PluginsDispatcher', 'MyDAGFactory'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('LeftPanelStore', LeftPanelStore);

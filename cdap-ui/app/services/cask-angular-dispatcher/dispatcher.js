/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

angular.module(PKG.name+'.services').factory('CaskAngularDispatcher', function(uuid) {
  function Dispatcher() {
    if (!(this instanceof Dispatcher)) {
      return new Dispatcher();
    }
    this.events = {};
  }

  Dispatcher.prototype.register = function(event, cb) {
    var id = uuid.v4();
    if (!this.events[event]) {
      this.events[event] = {};
    }
    if (typeof cb === 'function') {
      this.events[event][id] = cb;
    } else {
      throw 'Invalid callback. A callback registered for an event has to be a function';
    }
    return id;
  };

  Dispatcher.prototype.dispatch =  function(event) {
    var args = Array.prototype.slice.call(arguments, 1);
    if (!this.events[event]) {
      return;
    }
    angular.forEach(this.events[event], function(callback) {
      callback.apply(null, args);
    });
  };

  Dispatcher.prototype.unregister = function(event, registeredId) {
    if (this.events[event] && this.events[event][registeredId]) {
      delete this.events[event][registeredId];
    }
  };

  return Dispatcher;

});

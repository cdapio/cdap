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

class HydratorPlusPlusConsoleTabCtrl {
  constructor(HydratorPlusPlusConsoleStore, myAlertOnValium) {
    this.HydratorPlusPlusConsoleStore = HydratorPlusPlusConsoleStore;
    this.myAlertOnValium = myAlertOnValium;
    this.setMessages();
    this.HydratorPlusPlusConsoleStore.registerOnChangeListener( this.setMessages.bind(this) );
  }

  setMessages() {
    let messages = this.HydratorPlusPlusConsoleStore.getMessages();
    if (Array.isArray(messages) && !messages.length) {
      return;
    }
    let missingNodesList = [];
    let errorMessage = [];
    let successMessage = [];
    messages.forEach( message => {
      switch(message.type) {
        case 'NO-SINK-FOUND':
          missingNodesList.push('sinks(s)');
          break;
        case 'NO-SOURCE-FOUND':
          missingNodesList.push('source(s)');
          break;
        case 'STRAY-NODES':
          errorMessage.push(message.payload.nodes.map( node => node.plugin.label).join(', ') + ' - nodes missing connections');
          break;
        case 'success':
          successMessage.push(message.content);
          break;
        case 'error':
          errorMessage.push(message.content);
          break;
      }
    });
    if (missingNodesList.length) {
      errorMessage.push(`Missing ${missingNodesList.join(', ')} in the pipeline.`);
    }
    if (successMessage.length) {
      this.myAlertOnValium.show({
        type: 'success',
        content: successMessage.join(', ')
      });
    } else if (errorMessage.length) {
      this.myAlertOnValium.show({
        type: 'danger',
        templateUrl: '/assets/features/hydratorplusplus/templates/partial/error-template.html',
        templateScope: {
          content: errorMessage,
          currentIndex: 0,
          moveToNextIndex : function() {
            if(errorMessage.length > this.currentIndex) {
              this.currentIndex += 1;
            }
          },
          moveToPrevIndex : function() {
            if (this.currentIndex > 0) {
              this.currentIndex -= 1;
            }
          }
        }
      });
    }
  }
}

HydratorPlusPlusConsoleTabCtrl.$inject = ['HydratorPlusPlusConsoleStore', 'myAlertOnValium'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusConsoleTabCtrl', HydratorPlusPlusConsoleTabCtrl);

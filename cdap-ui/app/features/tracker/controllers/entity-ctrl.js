/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

class TrackerEntityController{
  constructor($state, $window, myJumpFactory) {
    this.$state = $state;
    this.$window = $window;
    this.myJumpFactory = myJumpFactory;

    let entityParams = this.$state.params.entityType;
    let entitySplit = entityParams.split(':');

    switch (entitySplit[0]) {
      case 'streams':
        this.entityInfo = {
          name: 'Stream',
          icon: 'icon-streams'
        };
        break;
      case 'datasets':
        this.entityInfo = {
          name: 'Dataset',
          icon: 'icon-datasets'
        };
        break;
      case 'views':
        this.entityInfo = {
          name: 'Stream View',
          icon: 'icon-streams'
        };
        break;
    }

  }

  goBack() {
    this.$window.history.back();
  }

  jump() {
    // let data = {
    //   'artifact': {
    //     'name': 'cdap-etl-batch',
    //     'scope': 'SYSTEM',
    //     'version': '3.4.0-SNAPSHOT'
    //   },
    //   'config': {
    //     'source': {
    //       'name': this.$state.params.entityId,
    //       'plugin': {
    //         'name': 'Stream',
    //         'properties': {
    //           'schema': '{ "name": "undefinedBody", "type": "record", "fields": [{"name": "something", "type": "string"}, { "name": "field2", "type": ["string", "null"] }] }',
    //           'name': this.$state.params.entityId,
    //           'format': 'csv'
    //         },
    //         'label': this.$state.params.entityId,
    //         'artifact': {
    //           'name': 'core-plugins',
    //           'scope': 'SYSTEM',
    //           'version': '1.3.0-SNAPSHOT'
    //         }
    //       }
    //     },
    //     'sinks': [],
    //     'transforms': [],
    //     'connections': []
    //   }
    // };

    // this.$state.go('hydrator.create.studio', {
    //   data: data,
    //   type: 'cdap-etl-batch'
    // });
    this.myJumpFactory.streamSource(this.$state.params.entityId);
  }
}

TrackerEntityController.$inject = ['$state', '$window', 'myJumpFactory'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerEntityController', TrackerEntityController);

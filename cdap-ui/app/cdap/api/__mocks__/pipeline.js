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

import { Subject } from 'rxjs/Subject';

const MyPipelineApi = {};

MyPipelineApi.publish = function() {};

MyPipelineApi.getPluginProperties = function() {
  const subject = new Subject();
  setTimeout(() => {
    subject.next([
      {
        name: 'someplugin',
      },
    ]);
  });
  return subject;
};

MyPipelineApi.fetchWidgetJson = function(params) {
  const subject = new Subject();

  const widget = {
    metadata: {
      'spec-version': '1.6',
    },
    'configuration-groups': [
      {
        label: 'group1',
        properties: [
          {
            'widget-type': 'textbox',
            label: 'prop-label',
            name: 'prop-name',
          },
        ],
      },
    ],
  };

  setTimeout(() => {
    subject.next({
      [params.keys]: JSON.stringify(widget),
    });
  });
  return subject;
};

module.exports = MyPipelineApi;

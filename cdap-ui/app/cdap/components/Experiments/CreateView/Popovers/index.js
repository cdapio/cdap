/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import { Provider } from 'react-redux';
import createExperimentStore from 'components/Experiments/store/createExperimentStore';
import NewExperimentPopover from 'components/Experiments/CreateView/Popovers/NewExperimentPopover';
import NewModelPopover from 'components/Experiments/CreateView/Popovers/NewModelPopover';

require('./ExperimentPopovers.scss');

export default function ExperimentPopovers() {
  return (
    <Provider store={createExperimentStore}>
      <div>
        <NewExperimentPopover />
        <NewModelPopover />
      </div>
    </Provider>
  );
}

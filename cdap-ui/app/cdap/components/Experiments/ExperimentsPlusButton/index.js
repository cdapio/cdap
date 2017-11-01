/*
 * Copyright © 2017 Cask Data, Inc.
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
import {Link} from 'react-router-dom';
import NamespaceStore from 'services/NamespaceStore';
import UncontrolledPopover from 'components/UncontrolledComponents/Popover.js';

require('./ExperimentsPlusButton.scss');

export default function ExperimentsPlusButton () {
  let popoverElement = (
    <img
      className="button-container"
      src="/cdap_assets/img/plus_ico.svg"
      id="experiments-plus-btn"
  />
  );
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  return (
    <UncontrolledPopover
      popoverElement={popoverElement}
      tag="span"
      className="experiments-plus-btn"
      tetherOption={{
        classPrefix: 'add-experiment-popover'
      }}
    >
      <Link to={`/ns/${namespace}/experiments/create`}>
        Create a new Experiment
      </Link>
      <hr />
      <div> More </div>
    </UncontrolledPopover>
  );
}

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

import React, {PropTypes} from 'react';
require('./AdminConfigurePane.less');
import ConfigureButton from '../ConfigureButton/index.js';
var shortid = require('shortid');

var configButtons = ['View Configurations', 'Add Namespace', 'Delete Namespace', 'Manage Namespaces', 'Instance Preference', 'Add Roles', 'Manage Roles', 'Reset Instance', 'Delete All Datasets', 'Reset Instance', 'Delete All Datasets', 'View Invalid Transactions', 'Tag Management'];

export default function AdminConfigurePane({ openNamespaceWizard }){

  var buttons = [];
  for(var i = 0 ; i < configButtons.length; i++){

    if(i === 1){
      buttons.push(
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[i]}
          onClick={openNamespaceWizard}
        />
      );
    } else {
      buttons.push(
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[i]}
        />
      );
    }
  }

  return (
    <div className="configure-pane">
      <span>Configure</span>
      <div className="configure-pane-container">
        {buttons}
      </div>
    </div>
  );
}

AdminConfigurePane.propTypes = {
  openNamespaceWizard : PropTypes.func
};

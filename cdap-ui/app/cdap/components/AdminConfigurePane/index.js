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
import T from 'i18n-react';

var configButtons = [
  T.translate('features.Management.Configure.buttons.add-ns'),
  T.translate('features.Management.Configure.buttons.view-config'),
  T.translate('features.Management.Configure.buttons.manage-ns'),
  T.translate('features.Management.Configure.buttons.delete-ns'),
  T.translate('features.Management.Configure.buttons.manage-roles'),
  T.translate('features.Management.Configure.buttons.reset-instance'),
  T.translate('features.Management.Configure.buttons.tag-management'),
  T.translate('features.Management.Configure.buttons.instance-preference'),
  T.translate('features.Management.Configure.buttons.delete-datasets'),
  T.translate('features.Management.Configure.buttons.view-invalid'),
];

export default function AdminConfigurePane({ openNamespaceWizard }){

  var buttons = [];
  for(var i = 0 ; i < configButtons.length; i++){

    if(i === 0){
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
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[0]}
          onClick={openNamespaceWizard}
          iconClass="icon-addnamespaces"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[1]}
          iconClass="icon-viewconfiguration"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[2]}
          iconClass="icon-managenamespaces"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[3]}
          iconClass="icon-deletenamespaces"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[4]}
          iconClass="icon-manageroles"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[5]}
          iconClass="icon-resetinstance"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[6]}
          iconClass="icon-tagmanagement"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[7]}
          iconClass="icon-instancepreference"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[8]}
          iconClass="icon-deletealldatasets"
        />
        <ConfigureButton
          key={shortid.generate()}
          label={configButtons[9]}
          iconClass="icon-viewinvalidtransactions"
        />
      </div>
    </div>
  );
}

AdminConfigurePane.propTypes = {
  openNamespaceWizard : PropTypes.func
};

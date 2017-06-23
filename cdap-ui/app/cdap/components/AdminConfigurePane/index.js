/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import ConfigureButton from '../ConfigureButton';
import shortid from 'shortid';
import classnames from 'classnames';
import T from 'i18n-react';
import ReloadSystemArtifacts from 'components/AdminConfigurePane/ReloadSystemArtifacts';
require('./AdminConfigurePane.scss');

export default function AdminConfigurePane({ openNamespaceWizard, openPreferenceModal, preferencesSavedState, closePreferencesSavedMessage }) {
  let setPreferencesLabel = T.translate('features.Administration.Configure.buttons.set-system-preferences');
  let setPreferenceSuccessLabel = T.translate('features.FastAction.setPreferencesSuccess.default', {entityType: 'CDAP'});
  let addNSLabel = T.translate('features.Administration.Configure.buttons.add-ns');
  let wrenchClasses = classnames('fa fa-wrench', {'preferences-saved-wrench': preferencesSavedState});

  return (
    <div className="configure-pane">
      <span>Configure</span>
      <div className="configure-pane-container">
        {
          preferencesSavedState ?
            <div className="preferences-saved-message text-white">
              <span>{setPreferenceSuccessLabel}</span>
              <span
                className='fa fa-times'
                onClick={closePreferencesSavedMessage}
              />
            </div>
          :
            null
        }
        <ConfigureButton
          key={shortid.generate()}
          label={setPreferencesLabel}
          onClick={openPreferenceModal}
          iconClass={wrenchClasses}
        />
        <ConfigureButton
          key={shortid.generate()}
          label={addNSLabel}
          onClick={openNamespaceWizard}
          iconClass="icon-addnamespaces"
        />

        <ReloadSystemArtifacts />
      </div>
    </div>
  );
}

AdminConfigurePane.propTypes = {
  openNamespaceWizard: PropTypes.func,
  openPreferenceModal: PropTypes.func,
  preferencesSavedState: PropTypes.bool,
  closePreferencesSavedMessage: PropTypes.func
};

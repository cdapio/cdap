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

import PropTypes from 'prop-types';
import React from 'react';
import { connect } from 'react-redux';
import {
  updateProfileLabel,
  updateProfileDescription,
} from 'components/Cloud/Profiles/CreateView/CreateProfileActionCreator';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import T from 'i18n-react';

require('./CreateProfileMetadata.scss');

const PREFIX = 'features.Cloud.Profiles.CreateView';

function ProfileName({ profileName }) {
  return (
    <WidgetWrapper
      pluginProperty={{
        required: true,
      }}
      widgetProperty={{
        'widget-type': 'textbox',
        label: T.translate(`${PREFIX}.profileName`).toString(),
        name: 'profileName',
        'widget-attributes': {
          placeholder: T.translate(`${PREFIX}.profileNamePlaceholder`).toString(),
        },
      }}
      value={profileName}
      disabled={true}
    />
  );
}
ProfileName.propTypes = {
  profileName: PropTypes.string,
};

const mapNameStateToProps = (state) => {
  return {
    profileName: state.name,
  };
};

function ProfileLabel({ profileLabel }) {
  return (
    <WidgetWrapper
      pluginProperty={{
        required: true,
      }}
      widgetProperty={{
        'widget-type': 'textbox',
        label: T.translate(`${PREFIX}.profileLabel`).toString(),
        name: 'profileLabel',
        'widget-attributes': {
          placeholder: T.translate(`${PREFIX}.profileLabelPlaceholder`).toString(),
        },
      }}
      value={profileLabel}
      onChange={updateProfileLabel}
    />
  );
}
ProfileLabel.propTypes = {
  profileLabel: PropTypes.string,
};

const mapLabelStateToProps = (state) => {
  return {
    profileLabel: state.label,
  };
};

const ConnectedProfileName = connect(mapNameStateToProps)(ProfileName);
const ConnectedProfileLabel = connect(mapLabelStateToProps)(ProfileLabel);
function ProfileDescription({ profileDescription }) {
  return (
    <WidgetWrapper
      pluginProperty={{
        required: true,
      }}
      widgetProperty={{
        'widget-type': 'textarea',
        label: T.translate('commons.descriptionLabel').toString(),
        name: 'profileDescription',
      }}
      value={profileDescription}
      onChange={updateProfileDescription}
    />
  );
}
ProfileDescription.propTypes = {
  profileDescription: PropTypes.string,
};

const mapDescriptionStateToProps = (state) => {
  return {
    profileDescription: state.description,
  };
};

const ConnectedProfileDescription = connect(mapDescriptionStateToProps)(ProfileDescription);

export { ConnectedProfileName, ConnectedProfileDescription, ConnectedProfileLabel };

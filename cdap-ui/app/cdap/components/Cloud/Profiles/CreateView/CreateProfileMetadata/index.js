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
import {connect} from 'react-redux';
import {Input} from 'reactstrap';
import {updateProfileName, updateProfileDescription} from 'components/Cloud/Profiles/CreateView/CreateProfileActionCreator';

require('./CreateProfileMetadata.scss');

function ProfileName({profileName}) {
  return (
    <Input
      aria-labelledby="profile-name"
      value={profileName}
      onChange={(e) => updateProfileName(e.target.value)}
      placeholder="Add a name for the Compute Profile"
      autoFocus
    />
  );
}
ProfileName.propTypes = {
  profileName: PropTypes.string
};

const mapNameStateToProps = (state) => {
  return {
    profileName: state.name
  };
};

const ConnectedProfileName = connect(mapNameStateToProps)(ProfileName);

function ProfileDescription({profileDescription}) {
  return (
    <Input
      type="textarea"
      aria-labelledby="profile-description"
      className="create-profile-description"
      value={profileDescription}
      onChange={(e) => updateProfileDescription(e.target.value)}
      placeholder="Add a description for the Compute Profile"
    />
  );
}
ProfileDescription.propTypes = {
  profileDescription: PropTypes.string
};

const mapDescriptionStateToProps = (state) => {
  return {
    profileDescription: state.description
  };
};

const ConnectedProfileDescription = connect(mapDescriptionStateToProps)(ProfileDescription);

export {ConnectedProfileName, ConnectedProfileDescription};

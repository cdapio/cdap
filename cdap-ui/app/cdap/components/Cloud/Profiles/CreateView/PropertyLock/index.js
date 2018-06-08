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
import PropTypes from 'prop-types';
import IconSVG from 'components/IconSVG';
import {togglePropertyLock} from 'components/Cloud/Profiles/CreateView/CreateProfileActionCreator';
import {objectQuery} from 'services/helpers';
import {connect} from 'react-redux';
import Popover from 'components/Popover';
require('./PropertyLock.scss');

function PropertyLock({isEditable, propertyName}) {
  let iconName = !isEditable ? 'icon-lock_close' : 'icon-lock_open';
  let title = isEditable
    ?
      'Click to disallow editing the value of this property after the profile is created'
    :
      'Click to allow editing the value of this property before running a program';
  const target = (
    <IconSVG
      className="property-lock"
      name={iconName}
      onClick={togglePropertyLock.bind(null, propertyName)}
    />
  );
  return (
    <Popover
      target={() => target}
      targetDimension={{
        width: 16,
        height: 21
      }}
      placement="right"
      showOn="Hover"
    >
      {title}
    </Popover>
  );
}

PropertyLock.propTypes = {
  isEditable: PropTypes.bool,
  propertyName: PropTypes.string
};

const mapStateToProps = (state, ownProps) => {
  let {propertyName} = ownProps;
  return {
    isEditable: objectQuery(state, 'properties', propertyName, 'isEditable'),
    propertyName
  };
};

const ConnectedPropertyLock = connect(mapStateToProps)(PropertyLock);

export default ConnectedPropertyLock;

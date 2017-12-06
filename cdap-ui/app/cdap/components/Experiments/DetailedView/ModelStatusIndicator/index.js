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

import PropTypes from 'prop-types';
import React from 'react';
import IconSVG from 'components/IconSVG';
require('./ModelStatusIndicator.scss');

const DEFAULT_STATUS_MAP = {
  className: 'text-info',
  icon: 'icon-circle'
};

const STATUS_ICON_MAP = {
  RUNNING: {
    className: 'text-success',
    icon: 'icon-circle'
  }
};
const getIconMap = (status) => status in STATUS_ICON_MAP ? STATUS_ICON_MAP[status]: DEFAULT_STATUS_MAP;

export default function ModelStatusIndicator({status}) {
  let iconMap = getIconMap(status);
  return (
    <span className="model-status-indicator">
      <IconSVG name={iconMap.icon} className={iconMap.className} />
      <span>{status}</span>
    </span>
  );
}
ModelStatusIndicator.propTypes = {
  status: PropTypes.string.isRequired
};

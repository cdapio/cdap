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
import classnames from 'classnames';
require('./IconSVG.scss');
require('../../styles/fonts/symbol-defs.svg');
require('../../styles/fonts/symbol-defs_new.svg');

export default function IconSVG(props) {
  const {name, className, ...moreProps} = props;
  const iconClassName = classnames('icon-svg', name, className);
  const path = `${window.location.href}#symbol-defs_${name}`;

  return (
    <svg
      className={iconClassName}
      {...moreProps}
    >
      <use xlinkHref={path} />
    </svg>
  );
}

IconSVG.propTypes = {
  name: PropTypes.string.isRequired,
  className: PropTypes.string,
  onClick: PropTypes.func
};

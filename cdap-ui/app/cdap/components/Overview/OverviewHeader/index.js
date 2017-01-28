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

import React, {PropTypes} from 'react';
import classnames from 'classnames';
import {Link} from 'react-router';
require('./OverviewHeader.scss');

export default function OverviewHeader({icon, title, linkTo, onClose}) {
  return (
    <div className="overview-header">
      <div className="header">
        <i className={classnames("fa", icon)} />
        <h4>{title}</h4>
      </div>
      {
        linkTo ?
          <Link
            className="link-to-detail"
            to={linkTo}
          >
            View Details
          </Link>
        :
          null
      }
      {
        onClose ?
          <span
            className="fa fa-times"
            onClick={onClose}
          >
          </span>
        :
          null
      }
    </div>
  );
}
OverviewHeader.propTypes = {
  icon: PropTypes.string,
  title: PropTypes.string,
  linkTo: PropTypes.object,
  onClose: PropTypes.func
};

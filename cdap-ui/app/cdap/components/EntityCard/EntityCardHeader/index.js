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

import React, {Component, PropTypes} from 'react';
import T from 'i18n-react';
require('./EntityCardHeader.scss');
import classnames from 'classnames';

const classNames = require('classnames');

export default class EntityCardHeader extends Component {
  constructor(props) {
    super(props);
  }

  getType() {
    if (this.props.entity.type === 'program') {
      return this.props.entity.programType.toLowerCase();
    } else if (this.props.entity.type !== 'application') {
      return this.props.entity.type;
    }

    if (this.props.systemTags.indexOf('cdap-data-pipeline') !== -1) {
      return 'cdap-data-pipeline';
    } else if (this.props.systemTags.indexOf('cdap-data-streams') !== -1) {
      return 'cdap-data-streams';
    } else {
      return this.props.entity.type;
    }
  }

  render() {
    return (
      <div
        onClick={this.props.onClick}
        className={classnames("entity-card-header", this.props.className)}
      >
        <h4>
          <span className={classNames('entity-icon', this.props.entity.icon)}></span>
          <span className="entity-type">
            {T.translate(`commons.entity.${this.getType()}.singular`)}
          </span>
        </h4>
      </div>
    );
  }
}

EntityCardHeader.defaultProps = {
  systemTags: []
};

EntityCardHeader.propTypes = {
  entity: PropTypes.object,
  systemTags: PropTypes.array,
  className: PropTypes.string,
  onClick: PropTypes.func
};

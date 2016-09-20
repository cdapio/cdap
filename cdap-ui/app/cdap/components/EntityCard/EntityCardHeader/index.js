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
require('./EntityCardHeader.less');

const classNames = require('classnames');

export default class EntityCardHeader extends Component {
  constructor(props) {
    super(props);
  }

  getType() {
    if (this.props.type !== 'application') {
      return this.props.type;
    }

    if (this.props.systemTags.includes('cdap-data-pipeline')) {
      return 'cdap-data-pipeline';
    } else if (this.props.systemTags.includes('cdap-data-streams')) {
      return 'cdap-data-streams';
    } else {
      return this.props.type;
    }
  }

  getEntityIcon(type) {
    const iconMap = {
      application: 'icon-fist',
      artifact: 'fa fa-archive',
      'cdap-data-pipeline': 'icon-ETLBatch',
      'cdap-data-streams': 'icon-sparkstreaming',
      datasetinstance: 'icon-datasets',
      stream: 'icon-streams',
      view: 'icon-streamview'
    };

    return iconMap[type];
  }


  render() {
    return (
      <div className="entity-card-header">
        <h4>
          <span className={classNames('entity-icon', this.getEntityIcon(this.getType()))}></span>
          <span className="entity-type">
            {T.translate(`commons.entity.${this.getType()}.singular`)}
          </span>
        </h4>
      </div>
    );
  }
}

EntityCardHeader.propTypes = {
  type: PropTypes.string,
  systemTags: PropTypes.array
};


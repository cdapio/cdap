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

import React, {PropTypes, Component} from 'react';
import {objectQuery} from 'services/helpers';
import FastActions from 'components/EntityCard/FastActions';
import isNil from 'lodash/isNil';
import Description from 'components/Description';
import TimeAgo from 'react-timeago';
import VersionsDropdown from 'components/VersionsDropdown';
import Tags from 'components/Tags';

require('./OverviewMetaSection.scss');

export default class OverviewMetaSection extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity
    };
  }
  componentWillReceiveProps(nextProps) {
    let {entity} = nextProps;
    if (
      !isNil(entity) &&
      entity.id !== objectQuery(this.state, 'entity', 'id')
    ) {
      this.setState({
        entity
      });
    }
  }
  onFastActionsUpdate() {}
  render() {
    let creationTime = objectQuery(this.props, 'entity', 'metadata', 'metadata', 'SYSTEM', 'properties', 'creation-time');
    let description =  objectQuery(this.props, 'entity', 'metadata', 'metadata', 'SYSTEM', 'properties', 'description');
    return (
      <div className="overview-meta-section">
        <h2>{this.props.entity.id}</h2>
        <div className="fast-actions-container">
          <div>
            {
              this.state.entity.type === 'application' ?
                <VersionsDropdown entity={this.props.entity} />
              :
                null
            }
            <small>
              <TimeAgo date={parseInt(creationTime, 10)} />
            </small>
          </div>
          <FastActions
            className="overview-fast-actions"
            entity={this.props.entity}
            onUpdate={this.onFastActionsUpdate.bind(this)}
          />
        </div>
        <Description description={description} />
        <Tags entity={this.props.entity} />
      </div>
    );
  }
}

OverviewMetaSection.propTypes = {
  entity: PropTypes.object
};

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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import Card from '../Card';
import EntityCardHeader from './EntityCardHeader';
import ApplicationMetrics from './ApplicationMetrics';
import ArtifactMetrics from './ArtifactMetrics';
import DatasetMetrics from './DatasetMetrics';
import ProgramMetrics from './ProgramMetrics';
import StreamMetrics from './StreamMetrics';
import classnames from 'classnames';
import FastActions from 'components/EntityCard/FastActions';
import FastActionToMessage from 'services/fast-action-message-helper';
import isNil from 'lodash/isNil';
import capitalize from 'lodash/capitalize';

require('./EntityCard.scss');

export default class EntityCard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      successMessage: null
    };
    this.cardRef = null;
    this.onSuccess = this.onSuccess.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.activeEntity !== this.props.entity.uniqueId) {
      this.setState({
        overviewMode: false
      });
    }
  }

  onSuccess(successMessage) {
    this.setState({successMessage});
    setTimeout(() => {
      this.setState({successMessage: null});
    }, 3000);
  }

  renderEntityStatus() {
    switch (this.props.entity.type) {
      case 'application':
        return <ApplicationMetrics entity={this.props.entity} />;
      case 'artifact':
        return <ArtifactMetrics entity={this.props.entity} />;
      case 'dataset':
        return <DatasetMetrics entity={this.props.entity} />;
      case 'program':
        return <ProgramMetrics entity={this.props.entity} poll={this.props.poll}/>;
      case 'stream':
        return <StreamMetrics entity={this.props.entity} />;
      case 'view':
        return null;
    }
  }

  onClick() {
    if (this.props.entity.type === 'artifact' || this.props.entity.type === 'program') {
      return;
    }

    if (this.props.onClick) {
      this.props.onClick();
    }
  }

  onFastActionUpdate(action) {
    let successMessage;
    if (action === 'setPreferences') {
      successMessage = FastActionToMessage(action, {entityType: capitalize(this.props.entity.type)});
    } else {
      successMessage = FastActionToMessage(action);
    }
    if (!isNil(successMessage)) {
      this.onSuccess(successMessage);
    }
    if (this.props.onUpdate) {
      this.props.onUpdate();
    }
  }

  render() {
    if (!this.props.entity) {
      return null;
    }
    const header = (
      <EntityCardHeader
        className={this.props.entity.isHydrator ? 'datapipeline' : this.props.entity.type}
        entity={this.props.entity}
        systemTags={this.props.entity.metadata.metadata.SYSTEM.tags}
        successMessage={this.state.successMessage}
      />
    );
    return (
      <div
        className={
          classnames(
            'entity-cards',
            this.props.entity.isHydrator ? 'datapipeline' : this.props.entity.type,
            this.props.className)
        }
        id={this.props.id}
        ref={(ref) => this.cardRef = ref}
      >
        <Card
          header={header}
          id={
            classnames(
              this.props.entity.isHydrator ?
              `entity-cards-datapipeline` :
              `entity-cards-${this.props.entity.type}`
            )
          }
          onClick={this.onClick.bind(this)}
        >
          <div className="entity-information clearfix">
            <div className={classnames("entity-id-container", {'with-version': this.props.entity.version})}>
              <h4
                className={classnames({'with-version': this.props.entity.version})}
                title={this.props.entity.id}
              >
                {this.props.entity.id}
              </h4>
              <small>{
                  this.props.entity.version ?
                    this.props.entity.version
                  :
                    ''
                }</small>
            </div>
          </div>

          {this.renderEntityStatus()}

          <div className="fast-actions-container text-xs-center">
            <FastActions
              entity={this.props.entity}
              onUpdate={this.onFastActionUpdate.bind(this)}
              onSuccess={this.props.onFastActionSuccess}
              className="btn-group"
            />
          </div>
        </Card>
      </div>
    );
  }
}

EntityCard.defaultProps = {
  onClick: () => {},
  poll: false
};

EntityCard.propTypes = {
  entity: PropTypes.object,
  poll: PropTypes.bool,
  onUpdate: PropTypes.func, // FIXME: Remove??
  onFastActionSuccess: PropTypes.func,
  className: PropTypes.string,
  id: PropTypes.string,
  onClick: PropTypes.func,
  activeEntity: PropTypes.string
};

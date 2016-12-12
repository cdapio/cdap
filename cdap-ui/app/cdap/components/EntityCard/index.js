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
import Card from '../Card';
import EntityCardHeader from './EntityCardHeader';
import ApplicationMetrics from './ApplicationMetrics';
import ArtifactMetrics from './ArtifactMetrics';
import DatasetMetrics from './DatasetMetrics';
import ProgramMetrics from './ProgramMetrics';
import StreamMetrics from './StreamMetrics';
import classnames from 'classnames';
import FastActions from 'components/EntityCard/FastActions';
import JumpButton from 'components/JumpButton';
import AppOverview from 'components/AppOverview';

require('./EntityCard.less');

export default class EntityCard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      overviewMode: false
    };
    this.cardRef = null;
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.activeEntity !== this.props.entity.uniqueId) {
      this.setState({
        overviewMode: false
      });
    }
  }

  renderEntityStatus() {
    switch (this.props.entity.type) {
      case 'application':
        return <ApplicationMetrics entity={this.props.entity} />;
      case 'artifact':
        return <ArtifactMetrics entity={this.props.entity} />;
      case 'datasetinstance':
        return <DatasetMetrics entity={this.props.entity} />;
      case 'program':
        return <ProgramMetrics entity={this.props.entity} poll={this.props.poll}/>;
      case 'stream':
        return <StreamMetrics entity={this.props.entity} />;
      case 'view':
        return null;
    }
  }

  renderJumpButton() {
    const entity = this.props.entity;
    if (['datasetinstance', 'stream'].indexOf(entity.type) === -1 && !entity.isHydrator) {
      return null;
    }

    return (
      <div className="jump-button-container text-center pull-right">
        <JumpButton
          entity={this.props.entity}
        />
      </div>
    );
  }

  toggleOverviewMode() {
    if (this.props.entity.type !== 'application') {
      return;
    }
    this.setState({
      overviewMode: !this.state.overviewMode
    });
    if (this.props.onClick) {
      this.props.onClick();
    }
  }

  render() {
    if (!this.props.entity) {
      return null;
    }
    const header = (
      <EntityCardHeader
        onClick={this.toggleOverviewMode.bind(this)}
        className={this.props.entity.isHydrator ? 'datapipeline' : this.props.entity.type}
        entity={this.props.entity}
        systemTags={this.props.entity.metadata.metadata.SYSTEM.tags}
      />
    );
    let position = 'left';
    let parentdimension = {};
    if (this.cardRef && this.state.overviewMode) {
      let cardDimension = this.cardRef.getBoundingClientRect();
      let parentDimension = this.cardRef.parentElement.getBoundingClientRect();
      let spaceOnLeft = cardDimension.left - parentDimension.left;
      let spaceOnRight = parentDimension.right - cardDimension.right;
      let spaceOnTop = cardDimension.top - parentDimension.top;
      let spaceOnBottom = parentDimension.bottom - cardDimension.bottom;
      let maxSpace = Math.max(spaceOnLeft, spaceOnRight, spaceOnBottom, spaceOnTop);
      parentdimension = parentDimension;
      // FIXME: ALERT! Magic number. This is the minimum width needed for the overview popover to look nice.
      // Definitely needs to be more adaptive & come from css.
      if (maxSpace < 400) {
        position = 'bottom';
      } else {
        if (spaceOnLeft === maxSpace) {
          position = 'left';
        }
        if (spaceOnRight === maxSpace) {
          position = 'right';
        }
        if (spaceOnBottom === maxSpace) {
          position = 'bottom';
        }
        if (spaceOnTop === maxSpace) {
          position = 'top';
        }
      }
    }
    return (
      <div
        className={
          classnames(
            'entity-cards',
            this.props.entity.isHydrator ? 'datapipeline' : this.props.entity.type,
            this.props.className)
        }
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
        >
          <div className="entity-information clearfix">
            <div className="entity-id-container">
              <h4
                className={classnames({'with-version': this.props.entity.version})}
              >
                {this.props.entity.id}
              </h4>
              <small>{
                  this.props.entity.version ?
                    this.props.entity.version
                  :
                    '1.0.0'
                }</small>
            </div>
            {this.renderJumpButton()}
          </div>

          {this.renderEntityStatus()}

          <div className="fast-actions-container">
            <FastActions
              entity={this.props.entity}
              onUpdate={this.props.onUpdate}
            />
          </div>
        </Card>
        {
          this.state.overviewMode ?
            <AppOverview
              onClose={this.toggleOverviewMode.bind(this)}
              position={position}
              parentdimension={parentdimension}
              entity={this.props.entity}
            />
          :
            null
        }
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
  onUpdate: PropTypes.func,
  className: PropTypes.string,
  onClick: PropTypes.func,
  activeEntity: PropTypes.string
};

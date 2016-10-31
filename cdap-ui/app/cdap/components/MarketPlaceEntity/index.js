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
import React, {PropTypes, Component} from 'react';
import Card from '../Card';
import {MyMarketApi} from 'api/market';
import classnames from 'classnames';
import TetherComponent from 'react-tether';
import MarketActionsContainer from 'components/MarketActionsContainer';
import AbstractWizard from 'components/AbstractWizard';

require('./MarketPlaceEntity.less');
export default class MarketPlaceEntity extends Component {
  constructor(props) {
    super(props);
    this.state = {
      expandedMode: false,
      entityDetail: {},
      performSingleAction: false
    };
  }
  getChildContext() {
    return {
      entity: this.props.entity
    };
  }

  fetchEntityDetail() {
    MyMarketApi.get({
      packageName: this.props.entity.name,
      version: this.props.entity.version
    }).subscribe((res) => {
      this.setState({entityDetail: res});
    }, (err) => {
      console.log('Error', err);
    });
  }
  openDetailedMode() {
    if (this.state.expandedMode) {
      return;
    }
    this.toggleDetailedMode();
  }
  toggleDetailedMode() {
    this.fetchEntityDetail();
    this.setState({expandedMode: !this.state.expandedMode});
  }
  render() {
    let verticalPosition = 'top';
    let horizontalPosition = 'left';
    let positionClassName='position-left';
    let cardWidth;
    const isEntityDetailAvailable = () => {
      if (!this.state.entityDetail || !Array.isArray(this.state.entityDetail.actions)) {
        return false;
      }
      return true;
    };
    if (this.packageCardRef) {
      let cardRects = this.packageCardRef.getBoundingClientRect();
      let parentRects = this.packageCardRef.parentElement.getBoundingClientRect();
      let activeTab = document.querySelector('.tab-content .tab-pane.active');
      cardWidth = 400;
      if (isEntityDetailAvailable() && this.state.entityDetail.actions.length > 1) {
        cardWidth = Math.max((parentRects.right - cardRects.left) , (cardRects.right - parentRects.left));
        cardWidth = cardWidth - 20;
      }
      let shouldPositionLeft = () => parentRects.right > (cardRects.left + cardWidth);
      let shouldPositionRight = () => parentRects.left < (cardRects.right - cardWidth);
      let shouldPositionTop = () => (cardRects.top - activeTab.top) > (activeTab.bottom - cardRects.bottom);
      if (shouldPositionLeft()) {
        horizontalPosition = 'left';
        positionClassName = 'position-left';
      } else if (shouldPositionRight()){
        horizontalPosition = 'right';
        positionClassName='position-right';
      }
      if (shouldPositionTop()) {
        verticalPosition = 'bottom';
      } else {
        verticalPosition = 'top';
      }
    }

    const getConsolidatedFooter = () => {
      if (isEntityDetailAvailable()) {
        if (this.state.entityDetail.actions.length > 1) {
          return (
            <div>
              <MarketActionsContainer
                actions={this.state.entityDetail.actions}
              />
              <div className="text-right">
                <button className="btn btn-default"
                  onClick={this.toggleDetailedMode.bind(this)}
                >
                  Cancel
                </button>
              </div>
            </div>
          );
        } else if (this.state.entityDetail.actions.length === 1) {
          return (
            <div className="text-right">
              <button
                className="btn btn-primary"
                onClick={() => this.setState({performSingleAction: true})}
              >
                Submit
                <AbstractWizard
                  isOpen={this.state.performSingleAction}
                  onClose={() => this.setState({performSingleAction: false})}
                  wizardType={this.state.entityDetail.actions[0].type}
                  input={{action: this.state.entityDetail.actions[0], package: this.props.entity}}
                />
              </button>
              <button className="btn btn-default" onClick={this.toggleDetailedMode.bind(this)}> Cancel </button>
            </div>
          );
        } else {
          return null;
        }
      }
    };

    return (

      <TetherComponent
        attachment={`${verticalPosition} ${horizontalPosition}`}
        targetAttachment={`${verticalPosition} ${horizontalPosition}`}
        className={classnames("market-place-package-card expanded", positionClassName)}
        constraints={[{to: 'scrollParent', attachment: 'together'}]}
        style={{'z-index': 1050, height: 0, margin: 0, width: cardWidth + 'px'}}
      >
        <div
          className="market-place-package-card"
          ref={(ref) => this.packageCardRef = ref}
          onClick={this.toggleDetailedMode.bind(this)}
        >
          <Card
            ref={(ref)=> this.cardRef = ref}
            onClick={this.toggleDetailedMode.bind(this)}
            size="LG"
          >
            <div
              className="package-icon-container"
              onClick={this.toggleDetailedMode.bind(this)}>
              <img src={MyMarketApi.getIcon(this.props.entity)} />
            </div>
            <div onClick={this.toggleDetailedMode.bind(this)}>
              <div>{this.props.entity.version}</div>
              <div>{this.props.entity.name}</div>
            </div>
          </Card>
        </div>
        {
          this.state.expandedMode ?
            <Card
              ref={(ref)=> this.cardRef = ref}
              size="LG"
              cardClass={classnames("market-place-package-card")}
              style={this.props.style}
              onClick={this.openDetailedMode.bind(this)}
            >
              <div className="clearfix">
                <div
                  className="package-icon-container"
                  onClick={this.toggleDetailedMode.bind(this)}>
                  <img src={MyMarketApi.getIcon(this.props.entity)} />
                </div>

                <div className="package-medata-container">
                  <strong className="package-label"> {this.props.entity.label} </strong>
                  <div className="package-metadata">
                    <div>
                      <span>
                        <strong> Version </strong>
                      </span>
                      <span> {this.props.entity.version} </span>
                    </div>
                    <div>
                      <span>
                        <strong> Organization </strong>
                      </span>
                      <span> {this.props.entity.org} </span>
                    </div>
                    <div>
                      <span>
                        <strong> Author </strong>
                      </span>
                      <span> {this.props.entity.author} </span>
                    </div>
                  </div>
                </div>
              </div>
              <div className="pacakge-footer">
                <p>
                  {this.props.entity.description}
                </p>
                { getConsolidatedFooter() }
              </div>
            </Card>
          :
            null
          }
      </TetherComponent>
    );
  }
}

MarketPlaceEntity.childContextTypes = {
  entity: PropTypes.shape({
    name: PropTypes.string,
    version: PropTypes.string,
    label: PropTypes.string,
    author: PropTypes.string,
    description: PropTypes.string,
    org: PropTypes.string,
    created: PropTypes.number,
    cdapVersion: PropTypes.string
  })
};

MarketPlaceEntity.propTypes = {
  className: PropTypes.string,
  style: PropTypes.object,
  entity: PropTypes.shape({
    name: PropTypes.string,
    version: PropTypes.string,
    label: PropTypes.string,
    author: PropTypes.string,
    description: PropTypes.string,
    org: PropTypes.string,
    created: PropTypes.number,
    cdapVersion: PropTypes.string
  })
};

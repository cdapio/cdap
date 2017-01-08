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
import MarketActionsContainer from 'components/MarketActionsContainer';
import AbstractWizard from 'components/AbstractWizard';
import ReactCSSTransitionGroup from 'react-addons-css-transition-group';
import MarketStore from 'components/Market/store/market-store';
import T from 'i18n-react';

require('./MarketPlaceEntity.less');
export default class MarketPlaceEntity extends Component {
  constructor(props) {
    super(props);
    this.state = {
      expandedMode: false,
      entityDetail: {},
      performSingleAction: false
    };
    this.unsub = MarketStore.subscribe(() => {
      let marketState = MarketStore.getState();
      if ((marketState.activeEntity !== this.props.entityId) && this.state.expandedMode) {
        this.setState({
          expandedMode: false
        });
      }
    });
    this.toggleDetailedMode = this.toggleDetailedMode.bind(this);
  }
  componentWillUnmount() {
    this.unsub();
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
      this.toggleDetailedMode();
    }, (err) => {
      console.log('Error', err);
    });
  }
  openDetailedMode() {
    if (this.state.expandedMode) {
      return;
    }
    this.fetchEntityDetail();
  }
  toggleDetailedMode() {
    this.setState({expandedMode: !this.state.expandedMode});
    MarketStore.dispatch({
      type: 'SET_ACTIVE_ENTITY',
      payload: {
        entityId: this.props.entityId
      }
    });
  }
  render() {
    const isEntityDetailAvailable = () => {
      if (!this.state.entityDetail || !Array.isArray(this.state.entityDetail.actions)) {
        return false;
      }
      return true;
    };

    // FIXME: This could be moved to a utility function. This can be generic.
    let style = {
      position: 'absolute'
    };
    let positionClassName;
    let cardWidth = 420;

    if (this.packageCardRef) {
      let parentRects = this.packageCardRef.parentElement.getBoundingClientRect();
      let cardRects = this.packageCardRef.getBoundingClientRect();
      if (isEntityDetailAvailable()) {
        if (this.state.entityDetail.actions.length > 1) {
          cardWidth = Math.max((parentRects.right - cardRects.left), (cardRects.right - parentRects.left));
        }
      }
      cardWidth = cardWidth - 20;
      let shouldPositionLeft = () => parentRects.right > (cardRects.left + (cardWidth - 20));
      let shouldPositionRight = () => parentRects.left < (cardRects.right - (cardWidth - 20));

      if (shouldPositionLeft()) {
          positionClassName = 'position-left';
      } else if (shouldPositionRight()) {
          positionClassName = 'position-right';
      }
    }
    style.width = cardWidth;
    const getConsolidatedFooter = () => {
      if (isEntityDetailAvailable()) {
        if (this.state.entityDetail.actions.length > 1) {
          return (
            <div>
              <MarketActionsContainer
                actions={this.state.entityDetail.actions}
              />
              <div className="text-right">
                <button
                  className="btn btn-default"
                  onClick={this.toggleDetailedMode}
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
                {T.translate('features.Market.action-types.' + this.state.entityDetail.actions[0].type + '.name')}
                <AbstractWizard
                  isOpen={this.state.performSingleAction}
                  onClose={() => this.setState({performSingleAction: false})}
                  wizardType={this.state.entityDetail.actions[0].type}
                  input={{action: this.state.entityDetail.actions[0], package: this.props.entity}}
                />
              </button>
              <button
                className="btn btn-default"
                onClick={this.toggleDetailedMode}
              >
                Cancel
              </button>
            </div>
          );
        } else {
          return null;
        }
      }
    };

    const getRightCard = () => {

      let beta = classnames('package-icon-container', {'beta' : this.props.entity.beta});

      return !this.state.expandedMode ?
        (
          <Card
            ref={(ref)=> this.cardRef = ref}
            onClick={this.openDetailedMode.bind(this)}
            size="LG"
          >
            {
              this.props.entity.beta ?
                <div className="experimental-banner">BETA</div>
              :
                null
            }
            <div className={beta}>
              <img src={MyMarketApi.getIcon(this.props.entity)} />
            </div>
            <div className="package-metadata-container">
              <strong className="package-label">{this.props.entity.label}</strong>
              <div>v {this.props.entity.version}</div>
            </div>
          </Card>
        )
      :
        (
          <Card
            ref={(ref)=> this.cardRef = ref}
            size="LG"
            cardStyle={style}
            onClick={this.openDetailedMode.bind(this)}
          >
            {
              this.props.entity.beta ?
                <div className="experimental-banner">BETA</div>
              :
                null
            }
            <div className="text-center">
              <div
                className={beta}>
                <img src={MyMarketApi.getIcon(this.props.entity)} />
              </div>

              <div className="package-metadata-container text-left">
                <strong className="package-label"> {this.props.entity.label} </strong>
                <div className="package-metadata">
                  <div>
                    <span>
                      <strong> {T.translate('features.MarketPlaceEntity.Metadata.version')} </strong>
                    </span>
                    <span> {this.props.entity.version} </span>
                  </div>
                  <div>
                    <span>
                      <strong> {T.translate('features.MarketPlaceEntity.Metadata.company')} </strong>
                    </span>
                    <span> {this.props.entity.org} </span>
                  </div>
                  <div>
                    <span>
                      <strong> {T.translate('features.MarketPlaceEntity.Metadata.author')} </strong>
                    </span>
                    <span> {this.props.entity.author} </span>
                  </div>
                  <div>
                    <span>
                      <strong> {T.translate('features.MarketPlaceEntity.Metadata.cdapversion')} </strong>
                    </span>
                    <span> {this.props.entity.cdapVersion} </span>
                  </div>
                </div>
              </div>
            </div>
            <div className="package-footer">
              <p>
                {this.props.entity.description}
              </p>
              { getConsolidatedFooter() }
            </div>
          </Card>
        );
    };

    return (
      <div
        className={classnames("market-place-package-card", {[positionClassName + ' expanded']: this.state.expandedMode})}
        ref={(ref)=> this.packageCardRef = ref}
      >
        <ReactCSSTransitionGroup
          transitionName="package-transition"
          transitionAppearTimeout={300}
          transitionEnterTimeout={500}
          transitionLeaveTimeout={300}
        >
          {getRightCard()}
        </ReactCSSTransitionGroup>
      </div>
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
  entityId: PropTypes.string,
  entity: PropTypes.shape({
    name: PropTypes.string,
    version: PropTypes.string,
    label: PropTypes.string,
    author: PropTypes.string,
    description: PropTypes.string,
    org: PropTypes.string,
    created: PropTypes.number,
    cdapVersion: PropTypes.string,
    beta: PropTypes.bool
  })
};

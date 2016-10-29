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

require('./MarketPlaceEntity.less');
export default class MarketPlaceEntity extends Component {
  constructor(props) {
    super(props);
    this.state = {
      expandedMode: false,
      entityDetail: {}
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
    if (this.packageCardRef) {
      let cardRects = this.packageCardRef.getBoundingClientRect();
      let parentRects = this.packageCardRef.parentElement.getBoundingClientRect();
      if (parentRects.right < (cardRects.left + 400)) {
        verticalPosition = 'top';
        horizontalPosition = 'right';
        positionClassName = 'position-right';
      }
    }

    return (

      <TetherComponent
        attachment={`${verticalPosition} ${horizontalPosition}`}
        targetAttachment={`${verticalPosition} ${horizontalPosition}`}
        className={classnames("market-place-package-card expanded", positionClassName)}
        constraints={[{to: 'scrollParent', attachment: 'together'}]}
        style={{'z-index': 1500, height: 0, margin: 0}}
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
                <div className="text-right">
                  <button className="btn btn-primary"> Submit </button>
                  <button className="btn btn-default" onClick={this.toggleDetailedMode.bind(this)}> Cancel </button>
                </div>
              </div>
            </Card>
          :
            null
          }

      </TetherComponent>
    );
  }
}

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

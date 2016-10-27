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
import {MyMarketApi} from '../../api/market';
import Card from 'components/Card';
import moment from 'moment';
require('./MarketPlaceUsecaseEntity.less');

export default class MarketPlaceUsecaseEntity extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showActions: false
    };
  }
  getVersion() {
    const versionElem = (
      <span>
        <strong>
          {this.props.entity.cdapVersion}
        </strong>
      </span>
    );

    return this.props.entity.cdapVersion ? versionElem : null;
  }

  render() {
    return (
      <Card
        size="LG"
        cardClass="market-place-usecase-package-card"
        onClose={() => console.log('Use case card closed')}
      >
        <div className="title clearfix">
          <span className="pull-left">{this.props.entity.label}</span>
          <span className="pull-right">Version: {this.props.entity.version}</span>
        </div>
        <div className="entity-information">
          <div className="entity-modal-image">
            <img src={MyMarketApi.getIcon(this.props.entity)} />
          </div>
          <div className="entity-content">
            <div className="entity-description">
              {this.props.entity.description}
            </div>
            <div className="entity-metadata">
              <div>Author</div>
              <span>
                <strong>
                  {this.props.entity.author}
                </strong>
              </span>
              <div>Company</div>
              <span>
                <strong>
                  {this.props.entity.org}
                </strong>
              </span>
              <div>Created</div>
              <span>
                <strong>
                  {(moment(this.props.entity.created * 1000)).format('MM-DD-YYYY HH:mm A')}
                </strong>
              </span>
              {this.props.entity.cdapVersion ? <div>CDAP Version</div> : null}
              {this.getVersion()}
            </div>
          </div>
        </div>
      </Card>
    );
  }
}
MarketPlaceUsecaseEntity.propTypes = {
  className: PropTypes.string,
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

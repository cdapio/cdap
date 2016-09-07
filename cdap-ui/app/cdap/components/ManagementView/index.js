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
require('./ManagementView.less');
var classNames = require('classnames');
import InfoCard from '../InfoCard/index.js';
import ServiceStatusComponent from '../ServiceStatusComponent/index.js';
import ServiceStatusPanel from '../ServiceStatusPanel/index.js';
import AdminDetailPanel from '../AdminDetailPanel/index.js';
import ConfigureModule from '../ConfigureModule/index.js';
import OverviewModule from '../OverviewModule/index.js';

const propTypes = {
  data: PropTypes.object
};

class ManagementView extends Component {

  constructor(props) {
    super(props);
    this.state = {
      application: 'CDAP',
      lastUpdated: 15,
      navUnderlineClass: 'one',
      loading: true
    };
    this.interval = undefined;
    this.setToCDAP = this.setToCDAP.bind(this);
    this.setToYARN = this.setToYARN.bind(this);
    this.setToHBASE = this.setToHBASE.bind(this);
  }

  setToCDAP() {
    if(this.state.application !== 'CDAP'){
      this.setState(
        {
          application: 'CDAP',
          lastUpdated: 0,
          navUnderlineClass : 'one'
        }
      );
    }
  }

  setToYARN() {
    if(this.state.application !== 'YARN'){
      this.setState(
        {
          application: 'YARN',
          lastUpdated: 0,
          navUnderlineClass : 'two'
        }
      );
    }
  }

  setToHBASE() {
    if(this.state.application !== 'HBASE'){
      this.setState(
        {
          application: 'HBASE',
          lastUpdated: 0,
          navUnderlineClass : 'three'
        }
      );
    }
  }

  // FIXME: This for giving it a strcture. Eventually will be removed.
  // Simulates the page loading
  simulateLoading() {
    setTimeout( () => {
      this.setState({
        loading: false
      });
    }, 1500);
  }
  componentDidMount() {
    this.simulateLoading();
  }
  render () {

    var oneActive = classNames({ 'active' : this.state.navUnderlineClass === 'one' });
    var twoActive = classNames({ 'active' : this.state.navUnderlineClass === 'two' });
    var threeActive = classNames({ 'active' : this.state.navUnderlineClass === 'three' });

    return (
       <div className="management-view">
        <div className="top-panel">
          <div className="admin-row top-row">
            <InfoCard isLoading={this.state.loading} version={this.props.data.version} />
            <InfoCard isLoading={this.state.loading} uptime={this.props.data.uptime} />
            <ServiceStatusComponent/>
            <ServiceStatusPanel isLoading={this.state.loading} services={this.props.data.services} />
          </div>
          <div className="admin-row">
            <AdminDetailPanel isLoading={this.state.loading} applicationName={this.state.application} timeFromUpdate={this.state.lastUpdated} />
          </div>
          <div className="navigation-container">
            <ul>
              <li onClick={this.setToCDAP} className="one">
                <a className={oneActive}>CDAP</a>
              </li>
              <li onClick={this.setToYARN} className="two">
                <a className={twoActive}>YARN</a>
              </li>
              <li onClick={this.setToHBASE} className="three">
                <a className={threeActive}>HBASE</a>
              </li>
              <hr className={this.state.navUnderlineClass} />
            </ul>
          </div>
        </div>
        <ConfigureModule />
        <OverviewModule isLoading={this.state.loading} />
      </div>
    );
  }
}

ManagementView.propTypes = propTypes;

export default ManagementView;

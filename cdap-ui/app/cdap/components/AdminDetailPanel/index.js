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


import React, {PropTypes} from 'react';
require('./AdminDetailPanel.less');
import AdminMetadataPane from '../AdminMetadataPane/index.js';
import shortid from 'shortid';
import {humanReadableNumber} from 'services/helpers';
import T from 'i18n-react';

const propTypes = {
  applicationName: PropTypes.string,
  timeFromUpdate: PropTypes.string,
  isLoading: PropTypes.bool,
  clickLeftButton: PropTypes.func,
  clickRightButton: PropTypes.func,
  serviceData: PropTypes.object
};

function AdminDetailPanel({applicationName, timeFromUpdate, clickLeftButton, clickRightButton, serviceData}){

  let panelData = [];

  //Process the data so it can be easily consumed by child components
  for(let key in serviceData){
    if(serviceData.hasOwnProperty(key)){
      let category = key;

      //Construct Array from Object Category
      //Convert number into human readable text
      let pairs = [];
      Object.keys(serviceData[key]).map((item) => {

        let humanReadableNum;

        if(key === 'storage' || key === 'memory'){
          humanReadableNum = humanReadableNumber(serviceData[key][item], 'STORAGE');
        } else {
          humanReadableNum = humanReadableNumber(serviceData[key][item]);
        }

        pairs.push({
          'statName' : item,
          'statNum' : humanReadableNum
        });
      });

      panelData.push({
        'statsHeader' : category,
        'stats' : pairs
      });
    }
  }

  let panes = panelData.map((panel) => {
    if(panel.stats.length){
      return (
        <AdminMetadataPane
          statObject={panel}
          key={shortid.generate()}
        />
      );
    }
  });

  //Place vertical lines between panes
  for(let i = panes.length ; i >= 0 ; i--){
    if(i !== 0 && i != panes.length){
      panes.splice(i, 0, <div className="vertical-line" key={shortid.generate()}/>);
    }
  }

  let translatedApplicationName = applicationName ?
      T.translate(`features.Management.Component-Overview.headers.${applicationName}`)
      :
      <span className="fa fa-spinner" />;

  return (
    <div className="admin-detail-panel">
      <div onClick={clickLeftButton} className="admin-detail-panel-button-left">
        <i className="fa fa-chevron-left" aria-hidden="true" />
      </div>
      <div onClick={clickRightButton} className="admin-detail-panel-button-right">
        <i className="fa fa-chevron-right" aria-hidden="true" />
      </div>
      <div className="admin-detail-panel-header">
        <div className="admin-detail-panel-header-name">
          {translatedApplicationName}
        </div>
        <div className="admin-detail-panel-header-status">
          Last updated {timeFromUpdate} seconds ago
        </div>
      </div>
      <div className="admin-detail-panel-body">
        {panes}
      </div>
    </div>
  );
}

AdminDetailPanel.propTypes = propTypes;

export default AdminDetailPanel;

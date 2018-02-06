/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import {TAB_OPTIONS} from 'components/PipelineConfigurations/Store';

export default function ConfigurationsSidePanel({isDetailView, isPreview, isBatch, activeTab, onTabChange, showAdvancedTabs, toggleAdvancedTabs}) {
  return (
    <div className="configurations-side-panel">
      <div className="configurations-tabs">
        {
          isDetailView || isPreview ?
            <div
              className={classnames("configuration-tab", {"active": activeTab === TAB_OPTIONS.RUNTIME_ARGS})}
              onClick={onTabChange.bind(null, TAB_OPTIONS.RUNTIME_ARGS)}
            >
              Runtime Arguments
            </div>
          :
            null
        }
        {
          !isDetailView && isPreview ?
            <div
              className={classnames("configuration-tab", {"active": activeTab === TAB_OPTIONS.PREVIEW_CONFIG})}
              onClick={onTabChange.bind(null, TAB_OPTIONS.PREVIEW_CONFIG)}
            >
              Preview Config
            </div>
          :
            null
        }
        {
          isDetailView || isPreview ?
            <div
              className="configuration-tab toggle-advanced-options"
              onClick={toggleAdvancedTabs}
            >
              <IconSVG name={showAdvancedTabs ? "icon-caret-down" : "icon-caret-right"} />
              <span>Advanced options</span>
            </div>
          :
            null
        }
        {
          showAdvancedTabs || (!isDetailView && !isPreview) ?
            <div className="advanced-options">
              <div
                className={classnames("configuration-tab", {"active": activeTab === TAB_OPTIONS.PIPELINE_CONFIG})}
                onClick={onTabChange.bind(null, TAB_OPTIONS.PIPELINE_CONFIG)}
              >
                Pipeline Config
              </div>
              <div
                className={classnames("configuration-tab", {"active": activeTab === TAB_OPTIONS.ENGINE_CONFIG})}
                onClick={onTabChange.bind(null, TAB_OPTIONS.ENGINE_CONFIG)}
              >
                Engine Config
              </div>
              <div
                className={classnames("configuration-tab", {"active": activeTab === TAB_OPTIONS.RESOURCES, "disabled": isPreview})}
                onClick={onTabChange.bind(null, TAB_OPTIONS.RESOURCES)}
              >
                Resources
              </div>
              {
                isBatch ?
                  <div
                    className={classnames("configuration-tab", {"active": activeTab === TAB_OPTIONS.ALERTS, "disabled": isPreview})}
                    onClick={onTabChange.bind(null, TAB_OPTIONS.ALERTS)}
                  >
                    Alerts
                  </div>
                :
                  null
              }
            </div>
          :
            null
        }
      </div>
    </div>
  );
}

ConfigurationsSidePanel.propTypes = {
  isDetailView: PropTypes.bool,
  isPreview: PropTypes.bool,
  isBatch: PropTypes.bool,
  activeTab: PropTypes.string,
  onTabChange: PropTypes.func,
  showAdvancedTabs: PropTypes.bool,
  toggleAdvancedTabs: PropTypes.func
};

ConfigurationsSidePanel.defaultProps = {
  isDetailView: false,
  isPreview: false,
  isBatch: true,
  showAdvancedTabs: false
};

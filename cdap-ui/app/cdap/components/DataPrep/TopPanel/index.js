/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import Loadable from 'react-loadable';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import DataPrepStore from 'components/DataPrep/store';
import { objectQuery, preventPropagation } from 'services/helpers';
import cdapavsc from 'services/cdapavscwrapper';
import { directiveRequestBodyCreator } from 'components/DataPrep/helper';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import T from 'i18n-react';
import isNil from 'lodash/isNil';
import { Switch } from 'components/DataPrep/DataPrepContentWrapper';
import Popover from 'components/Popover';
import IconSVG from 'components/IconSVG';
import DataPrepPlusButton from 'components/DataPrep/TopPanel/PlusButton';
import { Theme } from 'services/ThemeHelper';
import classnames from 'classnames';

const SchemaModal = Loadable({
  loader: () =>
    import(/* webpackChunkName: "SchemaModal"*/ 'components/DataPrep/TopPanel/SchemaModal'),
  loading: LoadingSVGCentered,
});
const AddToPipelineModal = Loadable({
  loader: () =>
    import(
      /* webpackChunkName: "AddToPipelineModal" */ 'components/DataPrep/TopPanel/AddToPipelineModal'
    ),
  loading: LoadingSVGCentered,
});
const UpgradeModal = Loadable({
  loader: () =>
    import(/* webpackChunkName: "UpgradeModal" */ 'components/DataPrep/TopPanel/UpgradeModal'),
  loading: LoadingSVGCentered,
});
const IngestDataFromDataPrep = Loadable({
  loader: () =>
    import(
      /* webpackChunkName: "IngestDataFromDataPrep" */ 'components/DataPrep/TopPanel/IngestDataFromDataPrep'
    ),
  loading: LoadingSVGCentered,
});

require('./TopPanel.scss');
const PREFIX = 'features.DataPrep.TopPanel';

export default class DataPrepTopPanel extends Component {
  constructor(props) {
    super(props);

    let initialState = DataPrepStore.getState().dataprep;
    this.state = {
      workspaceModal: false,
      schemaModal: false,
      addToPipelineModal: false,
      upgradeModal: false,
      higherVersion: initialState.higherVersion,
      onSubmitError: null,
      onSubmitLoading: false,
      workspaceInfo: initialState.workspaceInfo,
    };

    this.toggleSchemaModal = this.toggleSchemaModal.bind(this);
    this.toggleAddToPipelineModal = this.toggleAddToPipelineModal.bind(this);
    this.toggleUpgradeModal = this.toggleUpgradeModal.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().dataprep;
      this.setState({
        higherVersion: state.higherVersion,
        workspaceInfo: state.workspaceInfo,
      });
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  toggleSchemaModal = () => {
    this.setState({ schemaModal: !this.state.schemaModal });
  };

  toggleAddToPipelineModal = () => {
    this.setState({ addToPipelineModal: !this.state.addToPipelineModal });
  };

  toggleUpgradeModal() {
    this.setState({ upgradeModal: !this.state.upgradeModal });
  }

  renderSchemaModal() {
    if (!this.state.schemaModal) {
      return null;
    }

    return <SchemaModal toggle={this.toggleSchemaModal} />;
  }

  renderAddToPipelineModal() {
    if (!this.state.addToPipelineModal) {
      return null;
    }

    return <AddToPipelineModal toggle={this.toggleAddToPipelineModal} />;
  }

  renderUpgradeModal() {
    if (!this.state.upgradeModal) {
      return null;
    }

    return <UpgradeModal toggle={this.toggleUpgradeModal} />;
  }

  onSubmit = () => {
    if (this.props.onSubmit) {
      let directives = DataPrepStore.getState().dataprep.directives;
      let workspaceId = DataPrepStore.getState().dataprep.workspaceId;
      let namespace = NamespaceStore.getState().selectedNamespace;
      let requestObj = {
        context: namespace,
        workspaceId,
      };
      let requestBody = directiveRequestBodyCreator(directives);
      this.setState({
        onSubmitLoading: true,
        onSubmitError: null,
      });
      MyDataPrepApi.getSchema(requestObj, requestBody).subscribe(
        (res) => {
          let schema = {
            name: 'avroSchema',
            type: 'record',
            fields: res,
          };
          try {
            cdapavsc.parse(schema, { wrapUnions: true });
          } catch (e) {
            this.setState({
              onSubmitError: e.message,
              onSubmitLoading: false,
            });
            return;
          }
          if (this.props.onSubmit) {
            this.props.onSubmit({
              workspaceId,
              directives,
              schema: JSON.stringify(schema),
            });
          }
        },
        (err) => {
          this.setState({
            onSubmitError: objectQuery(err, 'response', 'message') || JSON.stringify(err),
            onSubmitLoading: false,
          });
        }
      );
    }
  };

  menu = [
    {
      label: T.translate(`${PREFIX}.copyToCDAPDatasetBtn.btnLabel`),
      component: IngestDataFromDataPrep,
      iconName: 'icon-upload',
      shouldRender: () => !this.props.singleWorkspaceMode && Theme.showIngestData,
      disabled: () =>
        isNil(this.state.workspaceInfo) ||
        objectQuery(this.state, 'workspaceInfo', 'properties', 'connection') === 'upload',
      disabledTooltip: T.translate(`${PREFIX}.copyToCDAPDatasetBtn.disabledTooltip`),
    },
    {
      label: T.translate(`${PREFIX}.viewSchemaBtnLabel`),
      iconName: 'icon-info-circle',
      onClick: this.toggleSchemaModal,
    },
  ];

  renderTopPanelDisplay() {
    let info = this.state.workspaceInfo;

    if (info) {
      if (info.properties.connection === 'file') {
        return (
          <div className="data-prep-name">
            <div className="connection-type truncate" title={T.translate(`${PREFIX}.file`)}>
              {T.translate(`${PREFIX}.file`)}
            </div>
            <div className="title" title={info.properties.file}>
              {info.properties.file}
            </div>
          </div>
        );
      } else if (info.properties.connection === 'database') {
        return (
          <div className="data-prep-name">
            <div
              className="connection-type truncate"
              title={`${T.translate(`${PREFIX}.database`)} - ${info.properties.connectionid}`}
            >
              {T.translate(`${PREFIX}.database`)}
              <span className="connection-name">{`- ${info.properties.connectionid}`}</span>
            </div>
            <div className="title" title={info.properties.name}>
              {T.translate(`${PREFIX}.databaseTitle`, { name: info.properties.name })}
            </div>
          </div>
        );
      } else if (info.properties.connection === 'upload') {
        return (
          <div className="data-prep-name">
            <div className="connection-type truncate" title={T.translate(`${PREFIX}.upload`)}>
              {T.translate(`${PREFIX}.upload`)}
              <span className="connection-name">{info.properties.connectionid}</span>
            </div>
            <div className="title" title={info.properties.name}>
              {info.properties.name}
            </div>
          </div>
        );
      } else if (
        ['kafka', 's3', 'gcs', 'bigquery', 'spanner', 'adls'].indexOf(
          info.properties.connection
        ) !== -1
      ) {
        return (
          <div className="data-prep-name">
            <div
              className="connection-type truncate"
              title={`${T.translate(`${PREFIX}.${info.properties.connection}`)} - ${
                info.properties.connectionid
              }`}
            >
              {T.translate(`${PREFIX}.${info.properties.connection}`)}
              <span className="connection-name">{`- ${info.properties.connectionid}`}</span>
            </div>
            <div className="title" title={info.properties.name}>
              {info.properties.name}
            </div>
          </div>
        );
      }
    }

    return (
      <div className="data-prep-name">
        <strong>{T.translate(`${PREFIX}.title`)}</strong>
      </div>
    );
  }

  renderMenuItem = (menuItem) => {
    // This is to prevent items in the menu that doesn't make sense when rendered in pipeline view.
    if (menuItem.shouldRender && !menuItem.shouldRender()) {
      return null;
    }
    // Hanlding divider here as placing it under DropdownItem will make it clickable.
    if (menuItem.label === 'divider') {
      return <hr />;
    }
    const isDisabled = menuItem.disabled && menuItem.disabled();

    const getMenuItem = (menuItem) => {
      let { label, component: Component } = menuItem;
      return (
        <div>
          {Component && !isDisabled ? (
            <span>
              <IconSVG name={menuItem.iconName} />
              <Component />
            </span>
          ) : (
            <span>
              <IconSVG name={menuItem.iconName} />
              <span>{label}</span>
            </span>
          )}
        </div>
      );
    };

    const MenuItem = (props) => {
      return (
        <li
          className={classnames(`popover-menu-item ${menuItem.className}`, {
            disabled: isDisabled,
          })}
          title={menuItem.label}
          onClick={isDisabled ? preventPropagation : menuItem.onClick}
          {...props}
        >
          {getMenuItem(menuItem)}
        </li>
      );
    };

    if (isDisabled && menuItem.disabledTooltip) {
      return (
        <Popover target={MenuItem} placement="top" showOn="Hover">
          {menuItem.disabledTooltip}
        </Popover>
      );
    }
    return <MenuItem />;
  };
  renderMenu() {
    return (
      <Popover
        target={() => <span>{T.translate('features.DataPrep.TopPanel.more')}</span>}
        targetDimension={{ width: '31px', height: '31px' }}
        className="more-dropdown"
        placement="bottom"
      >
        <ul>
          {this.menu.map((menu, i) => {
            return <React.Fragment key={i}>{this.renderMenuItem(menu)}</React.Fragment>;
          })}
        </ul>
      </Popover>
    );
  }

  renderApplyBtn() {
    return (
      <button
        className="btn btn-primary"
        onClick={this.onSubmit}
        disabled={
          this.props.disabled || this.state.onSubmitLoading || isNil(this.state.workspaceInfo)
            ? 'disabled'
            : false
        }
      >
        {this.state.onSubmitLoading ? <IconSVG name="icon-spinner" className="fa-spin" /> : null}
        <span>{T.translate(`${PREFIX}.applyBtnLabel`)}</span>
      </button>
    );
  }

  renderUpgradeBtn() {
    return (
      <div className="upgrade-button">
        <button className="btn btn-info" onClick={this.toggleUpgradeModal}>
          <span className="fa fa-wrench fa-fw" />
          {T.translate(`${PREFIX}.upgradeBtnLabel`)}
        </button>
        {this.renderUpgradeModal()}
      </div>
    );
  }

  render() {
    return (
      <div className="row top-panel clearfix">
        <div className="left-title">
          <div className="upper-section">{this.renderTopPanelDisplay()}</div>
        </div>
        <Switch />

        <div className="action-buttons">
          {this.state.onSubmitError ? (
            <span className="text-danger">{this.state.onSubmitError}</span>
          ) : null}
          {this.state.higherVersion ? this.renderUpgradeBtn() : null}
          {this.props.singleWorkspaceMode ? this.renderApplyBtn() : null}
          {!this.props.singleWorkspaceMode ? (
            <button className="btn btn-primary" onClick={this.toggleAddToPipelineModal}>
              {T.translate(`${PREFIX}.addToPipelineBtnLabel`)}
            </button>
          ) : null}
          {this.renderMenu()}
          {!this.props.singleWorkspaceMode ? <DataPrepPlusButton /> : null}
          {this.renderAddToPipelineModal()}
          {this.renderSchemaModal()}
        </div>
      </div>
    );
  }
}

DataPrepTopPanel.propTypes = {
  singleWorkspaceMode: PropTypes.bool,
  onSubmit: PropTypes.func,
  disabled: PropTypes.bool,
};

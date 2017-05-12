/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, { Component, PropTypes } from 'react';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import LoadingSVG from 'components/LoadingSVG';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import T from 'i18n-react';

const PREFIX = 'features.DataPrepConnections.AddConnections.Database.DatabaseOptions';

const DB_LIST = {
  column1: [
    {
      id: 'db2',
      name: 'DB2 11',
      classname: 'db-db2',
    },
    {
      id: 'oracle',
      name: 'Oracle 12c',
      classname: 'db-oracle12c',
    },
    {
      id: 'MSSQL',
      name: 'MSSQL',
      classname: 'db-mssql',
    },
    {
      id: 'Netezza',
      name: 'Netezza',
      classname: 'db-netezza',
    }
  ],
  column2: [
    {
      id: 'MySQL',
      name: 'MySQL',
      classname: 'db-mysql',
    },
    {
      id: 'Teradata',
      name: 'Teradata',
      classname: 'db-teradata',
    },
    {
      id: 'PostGres',
      name: 'Postgresql',
      classname: 'db-postgres',
    },
    {
      id: 'OTHER',
      name: 'Other Database',
      classname: 'db-other',
    }
  ]
};

export default class DatabaseOptions extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: true,
      drivers: {}
    };

  }

  componentWillMount() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.listDrivers({namespace})
      .subscribe((res) => {
        this.mapUploadedArtifact(res);
      }, (err) => {
        console.log('Error fetching drivers list', err);
      });
  }

  mapUploadedArtifact(response) {
    let drivers = {};

    DB_LIST.column2.concat(DB_LIST.column1).forEach((db) => {
      if (response[db.id]) {
        drivers[db.id] = {
          installed: true,
          pluginInfo: response[db.id][0],
          database: db
        };
      } else {
        drivers[db.id] = {
          installed: false,
          database: db
        };
      }
    });

    this.setState({
      drivers,
      loading: false
    });
  }

  onDBClick(db) {
    if (!db.installed) { return; }

    this.props.onDBSelect(db);
  }

  renderDBInfo(db) {
    if (!db.installed) {
      return (
        <div className="db-installed">
          <span>{T.translate(`${PREFIX}.install`)}</span>
          <span className="upload">{T.translate(`${PREFIX}.upload`)}</span>
        </div>
      );
    }

    return (
      <div className="db-installed">
        <span>
          {db.pluginInfo.artifactVersion}
        </span>
        <span className="fa fa-fw check-icon">
          <IconSVG name="icon-check" />
        </span>
        <span>{T.translate(`${PREFIX}.installedLabel`)}</span>
      </div>
    );
  }

  renderDBOption(db) {
    let dbInfo = this.state.drivers[db.id];

    return (
      <div
        key={db.id}
        className={classnames('database-option', {'installed': dbInfo.installed})}
        onClick={this.onDBClick.bind(this, dbInfo)}
      >
        <div className={`db-image ${db.classname}`}></div>
        <div className="db-info">
          <div className="db-name">{db.name}</div>
          {this.renderDBInfo(dbInfo)}
        </div>
      </div>
    );
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="database-options text-xs-center">
          <LoadingSVG />
        </div>
      );
    }

    return (
      <div className="database-options">
        <div className="options-title">
          {T.translate(`${PREFIX}.optionsTitle`)}
        </div>

        <div className="row">
          <div className="col-xs-6">
            {
              DB_LIST.column1.map((db) => this.renderDBOption(db))
            }
          </div>
          <div className="col-xs-6">
            {
              DB_LIST.column2.map((db) => this.renderDBOption(db))
            }
          </div>
        </div>
      </div>
    );
  }
}

DatabaseOptions.propTypes = {
  onDBSelect: PropTypes.func
};


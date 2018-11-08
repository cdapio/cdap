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

import * as React from 'react';
import { connect } from 'react-redux';
import {
  listSpannerTables,
  listSpannerInstances,
  listSpannerDatabases,
  setSpannerLoading,
  setError,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import IconSVG from 'components/IconSVG';
import { Link } from 'react-router-dom';
import { match } from 'react-router';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { objectQuery } from 'services/helpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import T from 'i18n-react';
import MyDataPrepApi from 'api/dataprep';

const PREFIX = `features.DataPrep.DataPrepBrowser.SpannerBrowser`;

interface IMatchParams {
  connectionId: string;
  instanceId: string;
  databaseId: string;
}

interface ISpannerTableListViewProps {
  tableList: ISpannerTableObject[];
  connectionId: string;
  instanceId: string;
  databaseId: string;
  enableRouting: boolean;
  loading: boolean;
  match: match<IMatchParams>;
  onWorkspaceCreate: (workspaceId: string) => void;
}

interface ISpannerTableObject {
  name: string;
}

class SpannerTableListView extends React.PureComponent<ISpannerTableListViewProps> {
  public static defaultProps: Partial<ISpannerTableListViewProps> = {
    enableRouting: true,
  };

  public componentDidMount() {
    if (!this.props.enableRouting) {
      return;
    }

    const { connectionId, instanceId, databaseId } = this.props.match.params;

    listSpannerTables(connectionId, instanceId, databaseId);
  }

  private createWorkspace(tableId) {
    setSpannerLoading();

    const namespace = getCurrentNamespace();
    const connectionId =
      this.props.connectionId || objectQuery(this.props, 'match', 'params', 'connectionId');
    const instanceId =
      this.props.instanceId || objectQuery(this.props, 'match', 'params', 'instanceId');
    const databaseId =
      this.props.databaseId || objectQuery(this.props, 'match', 'params', 'databaseId');
    const params = {
      namespace,
      connectionId,
      instanceId,
      databaseId,
      tableId,
    };

    MyDataPrepApi.readSpannerTable(params).subscribe(
      (res) => {
        const workspaceId = objectQuery(res, 'values', 0, 'id');
        if (typeof this.props.onWorkspaceCreate === 'function') {
          this.props.onWorkspaceCreate(workspaceId);
          return;
        }
        window.location.href = `${
          window.location.origin
        }/cdap/ns/${namespace}/dataprep/${workspaceId}`;
      },
      (err) => {
        setError(err);
      }
    );
  }

  public render() {
    if (this.props.loading) {
      return <LoadingSVGCentered />;
    }

    const { tableList } = this.props;
    const connectionId =
      this.props.connectionId || objectQuery(this.props, 'match', 'params', 'connectionId');
    const instanceId =
      this.props.instanceId || objectQuery(this.props, 'match', 'params', 'instanceId');
    const databaseId =
      this.props.databaseId || objectQuery(this.props, 'match', 'params', 'databaseId');

    if (!tableList.length) {
      return (
        <div className="empty-search-container">
          <div className="empty-search text-center">
            <strong>
              {T.translate(`${PREFIX}.EmptyMessage.emptyTableList`, {
                connectionName: connectionId,
                instanceName: instanceId,
                name: databaseId,
              })}
            </strong>
          </div>
        </div>
      );
    }

    const Tag = this.props.enableRouting ? Link : 'span';
    const namespace = getCurrentNamespace();
    const instancePath = `/ns/${namespace}/connections/spanner/${connectionId}`;
    const databasePath = `${instancePath}/instances/${instanceId}`;

    return (
      <div className="list-view-container">
        <div className="sub-panel">
          <div className="path">
            <Tag
              to={instancePath}
              className="instance-path"
              onClick={listSpannerInstances.bind(null, connectionId)}
            >
              {T.translate(`${PREFIX}.instances`)}
            </Tag>
            <span> / </span>
            <Tag
              to={databasePath}
              className="database-path"
              onClick={listSpannerDatabases.bind(null, connectionId, instanceId)}
            >
              {instanceId}
            </Tag>
            <span> / </span>
            <span>{databaseId}</span>
          </div>
          <div>{T.translate(`${PREFIX}.tableCount`, { context: tableList.length })}</div>
        </div>

        <div className="list-table">
          <div className="table-header">
            <div className="row">
              <div className="col-12">{T.translate(`${PREFIX}.name`)}</div>
            </div>
          </div>

          <div className="table-body">
            {tableList.map((table: ISpannerTableObject) => {
              return (
                <div key={table.name} onClick={this.createWorkspace.bind(this, table.name)}>
                  <div className="row content-row">
                    <div className="col-12">
                      <IconSVG name="icon-table" />
                      {table.name}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state): Partial<ISpannerTableListViewProps> => {
  return {
    instanceId: state.spanner.instanceId,
    tableList: state.spanner.tableList,
    connectionId: state.spanner.connectionId,
    databaseId: state.spanner.databaseId,
    loading: state.spanner.loading,
  };
};

const SpannerTableList = connect(mapStateToProps)(SpannerTableListView);

export default SpannerTableList;

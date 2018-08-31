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
import {connect} from 'react-redux';
import {
  listSpannerTables,
  listSpannerInstances,
  listSpannerDatabases,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import IconSVG from 'components/IconSVG';
import {Link} from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {objectQuery} from 'services/helpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import T from 'i18n-react';

const PREFIX = `features.DataPrep.DataPrepBrowser.SpannerBrowser`;

interface IReactRouterMatch {
  params: {
    connectionId: string;
    instanceId: string;
    databaseId: string;
  };
}

interface ISpannerTableListViewProps {
  tableList: object[];
  connectionId: string;
  instanceId: string;
  databaseId: string;
  enableRouting: boolean;
  loading: boolean;
  match: IReactRouterMatch;
}

interface ISpannerTableObject {
  tableName: string;
}

class SpannerTableListView extends React.PureComponent<ISpannerTableListViewProps> {
  public static defaultProps: Partial<ISpannerTableListViewProps> = {
    enableRouting: true,
  };

  private clickHandler = (databaseId: string) => {
    if (this.props.enableRouting) { return; }
    // TODO: Read table to dataprep
  }

  public componentDidMount() {
    if (!this.props.enableRouting) { return; }

    const {
      connectionId,
      instanceId,
      databaseId,
    } = this.props.match.params;

    listSpannerTables(connectionId, instanceId, databaseId);
  }

  public render() {
    if (this.props.loading) {
      return <LoadingSVGCentered />;
    }

    const {connectionId, tableList} = this.props;
    const instanceId = this.props.instanceId || objectQuery(this.props, 'match', 'params', 'instanceId');
    const databaseId = this.props.databaseId || objectQuery(this.props, 'match', 'params', 'databaseId');

    if (!tableList.length) {
      return (
        <div className="empty-search-container">
          <div className="empty-search text-xs-center">
            <strong>
              {T.translate(`${PREFIX}.EmptyMessage.emptyTableList`, {
                connectionName: connectionId,
                instanceName: instanceId,
                tableName: databaseId,
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
            <span>{instanceId}</span>
            <span> / </span>
            <Tag
              to={databasePath}
              className="database-path"
              onClick={listSpannerDatabases.bind(null, connectionId, instanceId)}
            >
              {T.translate(`${PREFIX}.databases`)}
            </Tag>
            <span> / </span>
            <span>{databaseId}</span>
          </div>
          <div>
            {T.translate(`${PREFIX}.tableCount`, {context: tableList.length})}
          </div>
        </div>

        <div className="list-table">
          <div className="table-header">
            <div className="row">
              <div className="col-xs-12">
                {T.translate(`${PREFIX}.name`)}
              </div>
            </div>
          </div>

          <div className="table-body">
            {
              tableList.map((table: ISpannerTableObject) => {
                const ElemTag = this.props.enableRouting ? Link : 'div';
                const instanceUrl = `/ns/${namespace}/connections/spanner/${connectionId}/instances/${instanceId}`;
                const databaseUrl = `${instanceUrl}/databases/${databaseId}`;
                const tableUrl = `${databaseUrl}/tables/${table.tableName}`;

                return (
                  <ElemTag
                    key={table.tableName}
                    to={tableUrl}
                    onClick={this.clickHandler.bind(null, table.tableName)}
                  >
                    <div className="row content-row">
                      <div className="col-xs-12">
                        <IconSVG name="icon-table" />
                        {table.tableName}
                      </div>
                    </div>
                  </ElemTag>
                );
              })
            }
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    instanceId: state.spanner.instanceId,
    tableList: state.spanner.tableList,
    connectionId: state.spanner.connectionId,
    databaseId: state.spanner.databaseId,
    loading: state.spanner.loading,
  };
};

const SpannerTableList = connect(
  mapStateToProps,
)(SpannerTableListView);

export default SpannerTableList;

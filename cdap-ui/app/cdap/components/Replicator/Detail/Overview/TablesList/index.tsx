/*
 * Copyright © 2020 Cask Data, Inc.
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

import React, { useContext, useEffect, useState } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { DetailContext } from 'components/Replicator/Detail';
import {
  generateTableKey,
  getFullyQualifiedTableName,
  getTableDisplayName,
} from 'components/Replicator/utilities';
import { Map } from 'immutable';
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';
import TextField from '@material-ui/core/TextField';
import InputAdornment from '@material-ui/core/InputAdornment';
import Search from '@material-ui/icons/Search';
import debounce from 'lodash/debounce';
import Table from 'components/Table';
import TableHeader from 'components/Table/TableHeader';
import TableRow from 'components/Table/TableRow';
import TableCell from 'components/Table/TableCell';
import TableBody from 'components/Table/TableBody';
import IconSVG from 'components/IconSVG';
import MetricsQueryHelper from 'services/MetricsQueryHelper';
import { PROGRAM_INFO } from 'components/Replicator/constants';
import { MyMetricApi } from 'api/metric';
import {
  parseOverviewMetrics,
  INITIAL_DATA,
} from 'components/Replicator/Detail/Overview/TablesList/metricsParser';
import { compare } from 'natural-orderby';
import TableColumnGroup from 'components/Table/TableColumnGroup';
import ColumnGroup from 'components/Table/ColumnGroup';
import TimePeriodDropdown from 'components/Replicator/Detail/TimePeriodDropdown';
import capitalize from 'lodash/capitalize';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    grid: {
      height: 'calc(100% - 50px)', // 100% -subtitle height
    },
    subtitle: {
      marginTop: '10px',
      marginBottom: '10px',
      display: 'grid',
      gridTemplateColumns: '50% 50%',
      alignItems: 'center',

      '& > div:last-child': {
        textAlign: 'right',
      },
    },
    search: {
      width: '250px',

      '& input': {
        paddingTop: '10px',
        paddingBottom: '10px',
      },
    },
    SNAPSHOTTING: {
      color: theme.palette.blue[100],
    },
    REPLICATING: {
      color: theme.palette.blue[100],
    },
    FAILING: {
      color: theme.palette.red[100],
    },
    IDLE: {
      color: theme.palette.grey[200],
    },
  };
};

const TablesListView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const { name, tables, columns, offsetBasePath, timeRange } = useContext(DetailContext);

  const [statusMap, setStatusMap] = useState(Map<string, string>());
  const [filteredTables, setFilteredTables] = useState(sortTable(tables.toList()));
  const [search, setSearch] = useState('');
  const [tableMetricsMap, setTableMetricsMap] = useState({});

  function handleSearch(e) {
    setSearch(e.target.value);
  }

  function sortTable(unorderedTable) {
    return unorderedTable
      .sort((a, b) => {
        return compare()(a.get('table'), b.get('table'));
      })
      .toList();
  }

  const filterTableBySearch = debounce(() => {
    if (!search || search.length === 0) {
      setFilteredTables(sortTable(tables.toList()));
      return;
    }

    const filteredList = tables
      .filter((row) => {
        const normalizedTable = row.get('table').toLowerCase();
        const normalizedSearch = search.toLowerCase();

        return normalizedTable.indexOf(normalizedSearch) !== -1;
      })
      .toList();

    setFilteredTables(sortTable(filteredList));
  }, 300);

  // handle search query change
  useEffect(filterTableBySearch, [search, tables]);

  useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
    };

    interface IRequestBody {
      name: string;
      offsetBasePath?: string;
    }
    const body: IRequestBody = {
      name,
    };

    if (offsetBasePath && offsetBasePath.length > 0) {
      body.offsetBasePath = offsetBasePath;
    }

    // TODO: optimize polling
    // Don't poll when status is not running
    const statusPoll$ = MyReplicatorApi.pollTableStatus(params, body).subscribe((res) => {
      const tableStatus = res.tables;

      let status = Map<string, string>();
      tableStatus.forEach((tableInfo) => {
        const tableKey = generateTableKey(tableInfo);
        status = status.set(tableKey, tableInfo.state);
      });

      setStatusMap(status);
    });

    return () => {
      statusPoll$.unsubscribe();
    };
  }, [offsetBasePath]);

  useEffect(() => {
    if (tables.size === 0) {
      return;
    }

    const tags = {
      namespace: getCurrentNamespace(),
      app: name,
      worker: PROGRAM_INFO.programId,
      run: '*',
      instance: '*',
    };

    const tagsParams = MetricsQueryHelper.tagsToParams(tags);

    const metrics = [
      'inserts',
      'updates',
      'deletes',
      'errors',
      'latency.seconds',
      'data.processed.bytes',
    ]
      .map((metric) => {
        return `metric=user.dml.${metric}`;
      })
      .join('&');

    const start = `start=now-${timeRange}`;
    const end = 'end=now';
    const aggregate = 'aggregate=false';
    const groupBy = 'groupBy=ent';

    const params = [start, end, aggregate, groupBy, tagsParams, metrics].join('&');

    const metricsPoll$ = MyMetricApi.pollQueryTags({ params }).subscribe(
      (res) => {
        setTableMetricsMap(parseOverviewMetrics(res, tables.toList().toJS()));
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('err', err);
      }
    );

    return () => {
      if (metricsPoll$ && typeof metricsPoll$.unsubscribe === 'function') {
        metricsPoll$.unsubscribe();
      }
    };
  }, [timeRange, tables]);
  return (
    <div className={classes.root}>
      <div className={classes.subtitle}>
        <div>
          Replicating <strong>{tables.size}</strong> tables
        </div>
        <div>
          <TextField
            className={classes.search}
            value={search}
            onChange={handleSearch}
            variant="outlined"
            placeholder="Search by table name"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search />
                </InputAdornment>
              ),
            }}
          />
        </div>
      </div>

      <Table
        columnTemplate="30px 2fr 100px 1fr 1fr 1fr 1fr 100px 1fr 1fr 1fr"
        classes={{ grid: classes.grid }}
      >
        <TableHeader>
          <TableColumnGroup>
            <ColumnGroup gridColumn="4 / span 8">
              Activities in the <TimePeriodDropdown />
            </ColumnGroup>
          </TableColumnGroup>
          <TableRow>
            <TableCell>
              <IconSVG name="icon-circle" className={classes.IDLE} />
            </TableCell>
            <TableCell>Table name</TableCell>
            <TableCell textAlign="right">
              <div>No. columns</div>
              <div>replicated</div>
            </TableCell>
            <TableCell textAlign="right">
              <div>Data</div>
              <div>replicated</div>
            </TableCell>
            <TableCell textAlign="right">
              <div>No. events</div>
              <div>replicated</div>
            </TableCell>
            <TableCell textAlign="right">
              <div>Throughput</div>
              <div>(events/min)</div>
            </TableCell>
            <TableCell textAlign="right">
              <div>Latency</div>
              <div>(min)</div>
            </TableCell>
            <TableCell />
            <TableCell textAlign="right">Inserts</TableCell>
            <TableCell textAlign="right">Updates</TableCell>
            <TableCell textAlign="right">Deletes</TableCell>
          </TableRow>
        </TableHeader>

        <TableBody>
          {filteredTables.toList().map((row) => {
            const tableKey = generateTableKey(row);

            const tableColumns = columns.get(tableKey);
            const numColumns = tableColumns ? tableColumns.size : 0;
            const tableStatus = statusMap.get(tableKey) || 'IDLE';
            const icon = tableStatus === 'SNAPSHOTTING' ? 'icon-circle-o' : 'icon-circle';
            const tableDisplayName = getTableDisplayName(row);
            const tableMetricsKey = getFullyQualifiedTableName(row);
            const tableMetrics = tableMetricsMap[tableMetricsKey] || { ...INITIAL_DATA };

            return (
              <TableRow key={tableKey.toString()}>
                <TableCell>
                  <span title={capitalize(tableStatus)}>
                    <IconSVG name={icon} className={classes[tableStatus]} />
                  </span>
                </TableCell>
                <TableCell>{tableDisplayName}</TableCell>
                <TableCell textAlign="right">{numColumns === 0 ? 'All' : numColumns}</TableCell>
                <TableCell textAlign="right">{tableMetrics.dataReplicated}</TableCell>
                <TableCell textAlign="right">{tableMetrics.totalEvents}</TableCell>
                <TableCell textAlign="right">{tableMetrics.eventsPerMin}</TableCell>
                <TableCell textAlign="right">{tableMetrics.latency}</TableCell>
                <TableCell />
                <TableCell textAlign="right">{tableMetrics.inserts}</TableCell>
                <TableCell textAlign="right">{tableMetrics.updates}</TableCell>
                <TableCell textAlign="right">{tableMetrics.deletes}</TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
};

const TablesList = withStyles(styles)(TablesListView);
// const TablesList = detailContextConnect(StyledTablesList);
export default TablesList;

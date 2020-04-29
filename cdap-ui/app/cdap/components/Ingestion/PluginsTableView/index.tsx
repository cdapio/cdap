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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import { getIcon } from 'components/Ingestion/helpers';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import { getPluginDisplayName } from 'components/Ingestion/helpers';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    // root: { display: 'flex', flexDirection: 'column', alignItems: 'center', width: 700 },
    root: { width: '100%' },
    pluginsRow: { display: 'flex', flexDirection: 'row', flexWrap: 'wrap' },
    title: { display: 'flex', alignItems: 'center' },
    pluginCard: {
      display: 'flex',
      flexDirection: 'column',
      margin: '10px',
      alignItems: 'center',
      width: '250px',
      height: '255px',
      flexShrink: 0,
      justifyContent: 'space-around',
    },
    pluginImageContainer: {
      display: 'flex',
      alignItems: 'center',
    },
    tableText: {
      fontSize: '1rem',
    },
    tableRow: {
      width: '100%',
    },
    pluginImageBackground: {
      display: 'flex',
      width: '100%',
      minHeight: '128px',
      justifyContent: 'center',
      backgroundColor: theme.palette.grey[700],
    },
    tablePluginIcon: {
      width: '32px',
      height: 'auto',
    },
    tablePluginFAIcon: {
      fontSize: '32px',
    },
    ingestionHeader: {
      backgroundColor: theme.palette.grey[700],
      color: theme.palette.grey[100],
    },
    targetName: {
      cursor: 'pointer',
      margin: '0px 5px',
      color: theme.palette.blue[100],
    },
    targetsCell: { maxWidth: '50%' },
    sourceNameCell: { minWidth: '300px' },
    targetsCellHeader: { maxWidth: '50%' },
    pluginIcon: {
      width: '100px',
      height: 'auto',
    },
    pluginFAIcon: {
      fontSize: '64px',
    },
    cardTitle: {
      padding: '15px',
    },
    cardButtonsContainer: {
      display: 'flex',
      width: '100%',
      justifyContent: 'center',
    },
    targetsButton: {},
  };
};
interface IPlugin {
  name: string;
  artifact: { version: string };
  widgetJson: any;
}
interface ICodeEditorProps extends WithStyles<typeof styles> {
  plugins: IPlugin[];
  sinks: any;
  onSourceSinkSelect: any;
}

const PluginListView: React.FC<ICodeEditorProps> = ({
  classes,
  plugins,
  sinks,
  onSourceSinkSelect,
}) => {
  return (
    <div className={classes.root}>
      <Paper className={classes.sourceListTable}>
        <Table className={classes.table}>
          <TableHead className={classes.ingestionHeader}>
            <TableRow>
              <TableCell></TableCell>
              <TableCell className={classes.tableText}>Source Name</TableCell>
              <TableCell align="left" className={classes.tableText}>
                Target
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {plugins.map((plugin, i) => {
              const displayName = getPluginDisplayName(plugin);
              const iconData = objectQuery(plugin, 'widgetJson', 'icon', 'arguments', 'data');
              const matchedSinks = sinks.map((sink, idx) => {
                const sinkName = getPluginDisplayName(sink);
                return (
                  <span key={`${idx}-${sinkName}`}>
                    <span
                      className={classes.targetName}
                      onClick={() => onSourceSinkSelect(plugin, sink)}
                    >
                      {sinkName}
                    </span>
                    <If condition={idx !== sinks.length - 1}>
                      <span> | </span>
                    </If>
                  </span>
                );
              });

              return (
                <TableRow
                  key={`${i}-${displayName}`}
                  className={classnames(classes.tableRow, classes.tableText)}
                >
                  <TableCell className={classes.tableText}>
                    <If condition={iconData}>
                      <img className={classes.tablePluginIcon} src={iconData} />
                    </If>
                    <If condition={!iconData}>
                      <div
                        className={`${classes.tablePluginFAIcon} fa ${getIcon(
                          plugin.name.toLowerCase()
                        )}`}
                      />
                    </If>
                  </TableCell>
                  <TableCell className={classnames(classes.sourceNameCell, classes.tableText)}>
                    {displayName}
                  </TableCell>
                  <TableCell
                    className={classnames(classes.targetsCell, classes.tableText)}
                    align="left"
                  >
                    {matchedSinks}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </Paper>
    </div>
  );
};

const StyledPluginList = withStyles(styles)(PluginListView);

function PluginList(props) {
  return (
    <ThemeWrapper>
      <StyledPluginList {...props} />
    </ThemeWrapper>
  );
}

(PluginList as any).propTypes = {};

export default PluginList;

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

import { Fab } from '@material-ui/core';
import CircularProgress from '@material-ui/core/CircularProgress';
import { green } from '@material-ui/core/colors';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import CheckIcon from '@material-ui/icons/Check';
import SaveIcon from '@material-ui/icons/Save';
import clsx from 'clsx';
import { IPropertyShowConfig } from 'components/ConfigurationGroup/types';
import {
  createContextConnect,
  IConfigurationGroupInfo,
  ICreateContext,
  IWidgetInfo,
} from 'components/PluginJSONCreator/Create';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'flex',
      alignItems: 'center',
    },
    wrapper: {
      margin: theme.spacing(1),
      position: 'relative',
    },
    buttonSuccess: {
      backgroundColor: green[500],
      '&:hover': {
        backgroundColor: green[700],
      },
    },
    fabProgress: {
      color: green[500],
      position: 'absolute',
      top: -6,
      left: -6,
      zIndex: 1,
    },
    buttonProgress: {
      color: green[500],
      position: 'absolute',
      top: '50%',
      left: '50%',
      marginTop: -12,
      marginLeft: -12,
    },
    jsonImporter: {
      position: 'absolute' as 'absolute',
      bottom: '20%',
      width: '100%',
      borderTop: `1px solid ${theme.palette.grey['500']}`,
    },
    fileInput: {
      display: 'none',
    },
  };
};

const PluginJsonImporterView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  setDisplayName,
  setConfigurationGroups,
  setGroupToInfo,
  setGroupToWidgets,
  setWidgetToInfo,
  setWidgetToAttributes,
  setFilters,
  setFilterToName,
  setFilterToCondition,
  setFilterToShowList,
  setShowToInfo,
  setOutputName,
}) => {
  const [loading, setLoading] = React.useState(false);
  const [success, setSuccess] = React.useState(false);

  const buttonClassname = clsx({
    [classes.buttonSuccess]: success,
  });

  const handleButtonClick = () => {
    if (!loading) {
      setSuccess(false);
      setLoading(true);
      /*timer.current = setTimeout(() => {
        setSuccess(true);
        setLoading(false);
      }, 2000);*/
    }
  };

  function processFileUpload() {
    return (e) => {
      setLoading(true);
      const files = e.target.files;
      if (files.length > 0) {
        const reader = new FileReader();
        reader.readAsText(files[0]);
        let fileContent;
        reader.onload = (r) => {
          fileContent = r.target.result;
          populateCreatorView(fileContent);
        };
      } else {
        setLoading(false);
      }
    };
  }

  function populateCreatorView(fileContent) {
    try {
      const pluginJSON = JSON.parse(fileContent);

      setDisplayName(pluginJSON['display-name']);

      const newConfigurationGroups: string[] = [];
      const newGroupToInfo = {};
      const newGroupToWidgets = {};
      const newWidgetToInfo = {};
      const newWidgetToAttributes = {};
      const newFilters: string[] = [];
      const newFilterToName = {};
      const newFilterToCondition = {};
      const newFilterToShowList = {};
      const newShowToInfo = {};

      pluginJSON['configuration-groups'].forEach((groupObj) => {
        if (!groupObj || Object.keys(groupObj).length == 0) {
          return;
        }
        const groupLabel = groupObj.label;

        // generate a unique group ID
        const newGroupID = 'ConfigGroup_' + uuidV4();

        newConfigurationGroups.push(newGroupID);

        newGroupToInfo[newGroupID] = {
          label: groupLabel,
        } as IConfigurationGroupInfo;

        newGroupToWidgets[newGroupID] = [];

        const groupWidgets = groupObj.properties;
        groupWidgets.forEach((widgetObj) => {
          // generate a unique widget ID
          const newWidgetID = 'Widget_' + uuidV4();

          newGroupToWidgets[newGroupID].push(newWidgetID);

          const widgetInfo = {
            widgetType: widgetObj['widget-type'],
            label: widgetObj.label,
            name: widgetObj.name,
            ...(widgetObj['widget-category'] && { widgetCategory: widgetObj['widget-category'] }),
          } as IWidgetInfo;

          newWidgetToInfo[newWidgetID] = widgetInfo;

          if (
            widgetObj['widget-attributes'] &&
            Object.keys(widgetObj['widget-attributes']).length > 0
          ) {
            newWidgetToAttributes[newWidgetID] = widgetObj['widget-attributes'];
          }
        });
      });

      if (pluginJSON.filters) {
        pluginJSON.filters.forEach((filterObj) => {
          if (!filterObj || Object.keys(filterObj).length == 0) {
            return;
          }

          // generate a unique filter ID
          const newFilterID = 'Filter_' + uuidV4();

          newFilters.push(newFilterID);

          newFilterToName[newFilterID] = filterObj.name;
          newFilterToCondition[newFilterID] = filterObj.condition;

          newFilterToShowList[newFilterID] = [];

          if (filterObj.show) {
            filterObj.show.map((showObj) => {
              const newShowID = 'Show_' + uuidV4();

              newFilterToShowList[newFilterID].push(newShowID);

              newShowToInfo[newShowID] = {
                name: showObj.name,
                ...(showObj.type && { type: showObj.type }),
              } as IPropertyShowConfig;
            });
          }
        });
      }

      const newOutputName =
        pluginJSON.outputs && pluginJSON.outputs.length > 0 ? pluginJSON.outputs[0].name : '';

      setConfigurationGroups(newConfigurationGroups);
      setGroupToInfo(newGroupToInfo);
      setGroupToWidgets(newGroupToWidgets);
      setWidgetToInfo(newWidgetToInfo);
      setWidgetToAttributes(newWidgetToAttributes);
      setFilters(newFilters);
      setFilterToName(newFilterToName);
      setFilterToCondition(newFilterToCondition);
      setFilterToShowList(newFilterToShowList);
      setShowToInfo(newShowToInfo);
      setOutputName(newOutputName);

      setSuccess(true);
      setLoading(false);
    } catch (e) {
      setSuccess(false);
      setLoading(false);
    }
  }

  return (
    <div className={classes.jsonImporter} data-cy="widget-wrapper-container">
      <div className={classes.root}>
        <div className={classes.wrapper}>
          <input
            accept="json/*"
            id="raised-button-file"
            type="file"
            className={classes.fileInput}
            onChange={processFileUpload()}
          />
          <label htmlFor="raised-button-file">
            <Fab aria-label="save" component="span" color="primary" className={buttonClassname}>
              {success ? <CheckIcon /> : <SaveIcon />}
            </Fab>
            {loading && <CircularProgress size={68} className={classes.fabProgress} />}
          </label>
        </div>
      </div>
    </div>
  );
};

const StyledPluginJsonImporterView = withStyles(styles)(PluginJsonImporterView);
const PluginJsonImporter = createContextConnect(StyledPluginJsonImporterView);
export default PluginJsonImporter;

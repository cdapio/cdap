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
import PluginConfigDisplay from 'components/Replicator/ConfigDisplay/PluginConfigDisplay';
import classnames from 'classnames';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import ExpandMore from '@material-ui/icons/ExpandMore';
import ExpandLess from '@material-ui/icons/ExpandLess';
import { IPluginInfo, IPluginConfig } from 'components/Replicator/types';
import { IWidgetJson } from 'components/ConfigurationGroup/types';

const MAX_HEIGHT = 300;
const styles = (theme): StyleRules => {
  return {
    gridContainer: {
      display: 'grid',
      gridTemplateColumns: '45% 45%',
      gridColumnGap: '10%',
    },
    container: {
      paddingTop: '15px',
      maxHeight: `${MAX_HEIGHT}px`,
      overflow: 'hidden',
    },
    expanded: {
      maxHeight: 'initial',
    },
    noViewMoreToggle: {
      marginBottom: '25px',
    },
    sectionTitle: {
      marginBottom: '5px',
    },
    viewMoreContainer: {
      paddingTop: '15px',
      paddingBottom: '15px',
      backgroundColor: theme.palette.white[50],
      '& > div > span': {
        color: theme.palette.blue[100],
        cursor: 'pointer',
        '&:hover': {
          textDecoration: 'underline',
        },
      },
    },
    viewMoreText: {
      marginRight: '25px',
    },
  };
};

interface IConfigDisplayProps extends WithStyles<typeof styles> {
  sourcePluginInfo: IPluginInfo;
  targetPluginInfo: IPluginInfo;
  sourcePluginWidget: IWidgetJson;
  targetPluginWidget: IWidgetJson;
  sourceConfig: IPluginConfig;
  targetConfig: IPluginConfig;
}

const ConfigDisplayView: React.FC<IConfigDisplayProps> = ({
  classes,
  sourcePluginInfo,
  targetPluginInfo,
  sourcePluginWidget,
  targetPluginWidget,
  sourceConfig,
  targetConfig,
}) => {
  const [viewingMore, setViewingMore] = React.useState(false);
  const containerRef = React.useRef(null);
  const [shouldDisplayToggle, setShouldDisplayToggle] = React.useState(false);

  function toggleViewMore() {
    setViewingMore(!viewingMore);
  }

  function setDisplayViewMoreToggle() {
    const container = containerRef.current;

    if (container.scrollHeight > MAX_HEIGHT) {
      setShouldDisplayToggle(true);
    } else {
      setShouldDisplayToggle(false);
    }
  }

  React.useEffect(() => {
    setDisplayViewMoreToggle();

    const mo = new MutationObserver(setDisplayViewMoreToggle);
    mo.observe(containerRef.current, {
      childList: true,
      subtree: true,
    });

    window.addEventListener('resize', setDisplayViewMoreToggle);

    return () => {
      mo.disconnect();
      window.removeEventListener('resize', setDisplayViewMoreToggle);
    };
  }, []);

  return (
    <div className={classes.root}>
      <div
        className={classnames(classes.container, classes.gridContainer, {
          [classes.expanded]: viewingMore,
          [classes.noViewMoreToggle]: !shouldDisplayToggle,
        })}
        ref={containerRef}
      >
        <div>
          <Heading type={HeadingTypes.h6} className={classes.sectionTitle} label="SOURCE" />
          <PluginConfigDisplay
            pluginInfo={sourcePluginInfo}
            pluginWidget={sourcePluginWidget}
            pluginConfig={sourceConfig}
          />
        </div>

        <div>
          <Heading type={HeadingTypes.h6} className={classes.sectionTitle} label="TARGET" />
          <PluginConfigDisplay
            pluginInfo={targetPluginInfo}
            pluginWidget={targetPluginWidget}
            pluginConfig={targetConfig}
          />
        </div>
      </div>
      <If condition={shouldDisplayToggle}>
        <div className={`${classes.gridContainer} ${classes.viewMoreContainer}`}>
          <div>
            <span onClick={toggleViewMore}>
              <span className={classes.viewMoreText}>
                View {!viewingMore ? 'more' : 'less'} configurations
              </span>
              {viewingMore ? <ExpandLess /> : <ExpandMore />}
            </span>
          </div>
          <div>
            <span onClick={toggleViewMore}>
              <span className={classes.viewMoreText}>
                View {!viewingMore ? 'more' : 'less'} configurations
              </span>
              {viewingMore ? <ExpandLess /> : <ExpandMore />}
            </span>
          </div>
        </div>
      </If>
    </div>
  );
};

const ConfigDisplay = withStyles(styles)(ConfigDisplayView);
export default ConfigDisplay;

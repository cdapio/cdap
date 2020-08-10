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
import { ILogResponse, LogLevel as LogLevelEnum } from 'components/LogViewer/types';
import DataFetcher from 'components/LogViewer/DataFetcher';
import LogRow from 'components/LogViewer/LogRow';
import debounce from 'lodash/debounce';
import TopPanel, { TOP_PANEL_HEIGHT } from 'components/LogViewer/TopPanel';
import LogLevel from 'components/LogViewer/LogLevel';
import { extractErrorMessage } from 'services/helpers';
import Alert from 'components/Alert';
import LoadingSVG from 'components/LoadingSVG';
import Heading, { HeadingTypes } from 'components/Heading';
import T from 'i18n-react';

export function logsTableGridStyle(theme): StyleRules {
  return {
    row: {
      display: 'grid',
      gridTemplateColumns: '170px 80px 1fr',
      borderBottom: `1px solid ${theme.palette.grey[300]}`,
      lineHeight: '30px',
      fontSize: '14px',
    },
    cell: {
      paddingLeft: '10px',
      paddingRight: '10px',
    },
  };
}

const GRID_HEADER_HEIGHT = '40px';

const styles = (theme): StyleRules => {
  const tableStyles = logsTableGridStyle(theme);
  return {
    root: {
      height: '100%',
    },
    logsTableHeader: tableStyles.row,
    cell: {
      ...tableStyles.cell,
      fontWeight: 'bold',
      lineHeight: GRID_HEADER_HEIGHT,
    },
    logsContainer: {
      height: `calc(100% - ${TOP_PANEL_HEIGHT} - ${GRID_HEADER_HEIGHT})`,
      overflowY: 'auto',
      borderBottom: `1px solid ${theme.palette.grey[200]}`,
    },
    indicator: {
      height: '1px',
      content: '',
    },
    initLoadingContainer: {
      textAlign: 'center',
      paddingTop: '25px',
    },
    noLogsContainer: {
      paddingTop: '25px',
    },
    noLogsMessage: {
      textAlign: 'center',
    },
  };
};

interface ILogViewerProps extends WithStyles<typeof styles> {
  dataFetcher: DataFetcher;
  stopPoll?: boolean;
  onClose?: () => void;
}

interface ILogViewerState {
  logs: ILogResponse[];
  isFetching: boolean;
  isPolling: boolean;
  error?: string;
  initLoading: boolean;
}

const MAX_LOG_ROWS = 100;
const SCROLL_BUFFER = 10;
const POLL_FREQUENCY = 5000;

class LogViewerView extends React.PureComponent<ILogViewerProps, ILogViewerState> {
  private bottomIndicator;
  private io;
  private logsContainer;
  private pollTimeout;
  private scrollPosition = 0;
  private topIndicator;

  public constructor(props) {
    super(props);
    this.logsContainer = React.createRef();
    this.topIndicator = React.createRef();
    this.bottomIndicator = React.createRef();
  }

  public state = {
    logs: [],
    isFetching: true,
    isPolling: true,
    error: null,
    initLoading: true,
  };

  public componentDidMount() {
    this.init();
  }

  private init() {
    this.props.dataFetcher.init().subscribe(this.processFirstResponse, this.processError);
  }

  private processFirstResponse = (response) => {
    this.setState(
      {
        logs: response,
        initLoading: false,
      },
      () => {
        if (!this.state.isPolling) {
          return;
        }

        this.scrollToBottom();
        this.watchScroll();
        this.setIntersectionObserver();

        this.setState({ isFetching: false });
        this.pollTimeout = setTimeout(this.startPoll, POLL_FREQUENCY);
      }
    );
  };

  private processError = (err) => {
    const extractedMessage = extractErrorMessage(err);
    const errorMessage =
      typeof extractedMessage === 'string' ? extractedMessage : JSON.stringify(extractedMessage);

    this.setState({
      error: errorMessage,
      isPolling: false,
      isFetching: false,
    });
  };

  private dismissError = () => {
    this.setState({ error: null });
  };

  private setIntersectionObserver = () => {
    const options = {
      root: this.logsContainer.current,
      rootMargin: '10px',
      threshold: 1.0,
    };

    this.io = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting || entry.intersectionRatio !== 1) {
          return;
        }

        if (entry.target === this.bottomIndicator.current) {
          this.fetchNext();
        } else if (entry.target === this.topIndicator.current) {
          this.fetchPrev();
        }
      });
    }, options);

    const bottomTarget = this.bottomIndicator.current;
    const topTarget = this.topIndicator.current;
    this.io.observe(bottomTarget);
    this.io.observe(topTarget);
  };

  private watchScrollCallback = () => {
    const logsContainer = this.logsContainer.current;
    const currentScroll = logsContainer.scrollTop;

    if (currentScroll < this.scrollPosition - SCROLL_BUFFER) {
      this.stopPoll();
    }

    this.scrollPosition = currentScroll;
  };

  private watchScroll = () => {
    const logsContainer = this.logsContainer.current;
    logsContainer.addEventListener('scroll', this.watchScrollCallback);
  };

  private stopScrollWatch = () => {
    const logsContainer = this.logsContainer.current;
    logsContainer.removeEventListener('scroll', this.watchScrollCallback);
  };

  private scrollToBottom = () => {
    const logsContainer = this.logsContainer.current;
    logsContainer.scrollTop = logsContainer.scrollHeight;
    this.scrollPosition = logsContainer.scrollTop;
  };

  private startPoll = () => {
    if (this.props.stopPoll) {
      this.stopPoll();
      this.fetchNext(); // to guarantee after we fetch one more time to get latest logs
      return;
    }

    if (!this.state.isPolling) {
      this.setState({ isPolling: true });
    }

    this.props.dataFetcher.getNext().subscribe((res) => {
      if (res.length > 0) {
        const newLogs = this.state.logs.concat(res);
        this.setState({ logs: newLogs });
      }

      this.setState(
        {
          isFetching: false,
        },
        () => {
          this.scrollToBottom();
          this.trimTopExcessLogs();

          this.pollTimeout = setTimeout(this.startPoll, POLL_FREQUENCY);
        }
      );
    }, this.processError);
  };

  private stopPoll = () => {
    if (this.pollTimeout) {
      clearTimeout(this.pollTimeout);
      this.pollTimeout = null;
    }

    if (this.state.isPolling) {
      this.setState({
        isPolling: false,
        isFetching: false,
      });
    }
  };

  private fetchPrev = debounce(() => {
    if (this.state.isFetching || this.state.isPolling) {
      return;
    }
    this.setState({ isFetching: true });

    const logsContainer = this.logsContainer.current;
    const currentScrollHeight = logsContainer.scrollHeight;

    this.props.dataFetcher.getPrev().subscribe((res) => {
      if (res.length > 0) {
        const newLogs = res.concat(this.state.logs);
        this.setState({ logs: newLogs });
      }

      this.setState(
        {
          isFetching: false,
        },
        () => {
          // maintaining scroll position
          const updatedScrollHeight = logsContainer.scrollHeight;
          logsContainer.scrollTop = updatedScrollHeight - currentScrollHeight;

          const trimmedLogs = this.state.logs.slice(0, MAX_LOG_ROWS);

          // trim excess logs
          this.setState({
            logs: trimmedLogs,
          });
          this.props.dataFetcher.onLogsTrim(
            trimmedLogs[0],
            trimmedLogs[this.state.logs.length - 1]
          );
        }
      );
    }, this.processError);
  }, 100);

  private fetchNext = debounce(() => {
    if (this.state.isFetching || this.state.isPolling) {
      return;
    }
    this.setState({ isFetching: true });

    this.props.dataFetcher.getNext().subscribe((res) => {
      if (res.length > 0) {
        const newLogs = this.state.logs.concat(res);
        this.setState({ logs: newLogs });
      }

      this.setState(
        {
          isFetching: false,
        },
        this.trimTopExcessLogs
      );
    }, this.processError);
  }, 100);

  // Trimming the top entries of logs will change the scroll height. This function will calculate the
  // delta of the scroll height to maintain the view.
  private trimTopExcessLogs() {
    const logsContainer = this.logsContainer.current;
    const offset = logsContainer.scrollHeight - logsContainer.scrollTop;

    if (this.state.logs.length > MAX_LOG_ROWS) {
      this.setState(
        {
          logs: this.state.logs.slice(this.state.logs.length - MAX_LOG_ROWS),
        },
        () => {
          logsContainer.scrollTop = logsContainer.scrollHeight - offset;
          this.scrollPosition = logsContainer.scrollTop;

          this.props.dataFetcher.onLogsTrim(
            this.state.logs[0],
            this.state.logs[this.state.logs.length - 1]
          );
        }
      );
    }
  }

  private cleanUpWatchers = () => {
    if (this.io) {
      this.io.disconnect();
    }
    this.stopScrollWatch();
    this.stopPoll();
  };

  private getLatestLogs = () => {
    this.cleanUpWatchers();

    this.setState(
      {
        isFetching: true,
        isPolling: true,
        initLoading: true,
      },
      () => {
        this.props.dataFetcher.getLast().subscribe(this.processFirstResponse, this.processError);
      }
    );
  };

  private changeLogLevel = (level: LogLevelEnum) => {
    this.cleanUpWatchers();

    this.setState(
      {
        isFetching: true,
        isPolling: true,
        initLoading: true,
      },
      () => {
        this.props.dataFetcher
          .setLogLevel(level)
          .subscribe(this.processFirstResponse, this.processError);
      }
    );
  };

  private setIncludeSystemLogs = (includeSystemLogs: boolean) => {
    this.cleanUpWatchers();

    this.setState(
      {
        isFetching: true,
        isPolling: true,
        initLoading: true,
      },
      () => {
        this.props.dataFetcher
          .setIncludeSystemLogs(includeSystemLogs)
          .subscribe(this.processFirstResponse, this.processError);
      }
    );
  };

  private renderContent() {
    const { classes } = this.props;
    if (this.state.initLoading) {
      return (
        <div className={classes.initLoadingContainer}>
          <LoadingSVG />
        </div>
      );
    }

    if (this.state.logs.length === 0) {
      return (
        <div className={classes.noLogsContainer}>
          <Heading
            type={HeadingTypes.h4}
            label={T.translate('features.LogViewer.noLogsMessage')}
            className={classes.noLogsMessage}
          />
        </div>
      );
    }

    return (
      <React.Fragment>
        {this.state.logs.map((logObj, i) => {
          return <LogRow key={`${logObj.offset}-${i}`} logObj={logObj} />;
        })}
      </React.Fragment>
    );
  }

  public render() {
    const { classes } = this.props;
    return (
      <div className={classes.root}>
        <TopPanel
          dataFetcher={this.props.dataFetcher}
          isPolling={this.state.isPolling}
          getLatestLogs={this.getLatestLogs}
          setSystemLogs={this.setIncludeSystemLogs}
          onClose={this.props.onClose}
        />
        <div className={classes.logsTableHeader}>
          <div className={classes.cell}>Time</div>
          <div className={classes.cell}>
            <LogLevel dataFetcher={this.props.dataFetcher} changeLogLevel={this.changeLogLevel} />
          </div>
          <div className={classes.cell}>Message</div>
        </div>
        <div className={classes.logsContainer} ref={this.logsContainer}>
          <div ref={this.topIndicator} className={classes.indicator} />
          {this.renderContent()}
          <div ref={this.bottomIndicator} className={classes.indicator} id="bottom" />
        </div>

        <Alert
          showAlert={!!this.state.error}
          message={this.state.error}
          onClose={this.dismissError}
          type="error"
        />
      </div>
    );
  }
}

const LogViewer = withStyles(styles)(LogViewerView);
export default LogViewer;

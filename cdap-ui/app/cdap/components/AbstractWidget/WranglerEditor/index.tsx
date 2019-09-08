/*
 * Copyright © 2019 Cask Data, Inc.
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
import * as React from 'react';
import { withStyles, WithStyles } from '@material-ui/styles';
import CodeEditor from 'components/CodeEditor';
import Button from '@material-ui/core/Button';
import ThemeWrapper from 'components/ThemeWrapper';
import If from 'components/If';
import { Modal, ModalBody } from 'reactstrap';
import DataPrepHome from 'components/DataPrepHome';
import { preventPropagation, objectQuery } from 'services/helpers';
import LoadingSVG from 'components/LoadingSVG';
import { IWidgetProps } from 'components/AbstractWidget';
import IconSVG from 'components/IconSVG';
// This artifact will stay until we migrate dataprep to use css-in-js
require('./wrangler-modal.scss');

const styles = (theme) => {
  return {
    root: {
      width: '100%',
      paddingTop: '7px',
    },
    wrangleButton: {
      margin: '10px 0',
    },
    modalBtnClose: {
      height: '50px',
      width: '50px',
      boxShadow: 'none',
      border: 0,
      background: 'transparent',
      borderLeft: `1px solid ${theme.palette.grey['300']}`,
      fontWeight: 'bold' as 'bold',
      fontSize: '1.5rem',
      '&:hover': {
        background: theme.palette.blue['40'],
        color: 'white',
      },
    },
  };
};

interface IWranglerEditorWidgetAttributes {
  rows?: string;
  placeholder?: string;
}

interface IWranglerEditorProps
  extends IWidgetProps<IWranglerEditorWidgetAttributes>,
    WithStyles<typeof styles> {}

/**
 * Code editor doesn't play well with prop updates. The wrangler modal
 * updates the directives and we need to update that immediately as soon as
 * the user closes wrangler modal.
 *
 * If we have componentWillReceiveProps then we update the component at which
 * point the ace editor instance gets messed up.
 *
 * There has got to be a better way to do this. Until then CDAP-15684.
 */
interface IWranglerEditorState {
  showDataprepModal: boolean;
  reloadCodeEditor: boolean;
}

class WranglerEditor extends React.PureComponent<IWranglerEditorProps, IWranglerEditorState> {
  public state = {
    showDataprepModal: false,
    reloadCodeEditor: false,
  };

  public toggleDataprepModal = () => {
    this.setState({
      showDataprepModal: !this.state.showDataprepModal,
    });
  };

  public closeDataprepModal = (e?: React.MouseEvent<HTMLElement>) => {
    this.setState(
      {
        showDataprepModal: false,
        reloadCodeEditor: true,
      },
      () => {
        setTimeout(() => {
          this.setState({
            reloadCodeEditor: false,
          });
        });
      }
    );
    preventPropagation(e);
  };

  public updateDirectivesAndCloseModal = ({ workspaceId, directives, schema }) => {
    if (!workspaceId || !directives) {
      this.closeDataprepModal();
      return;
    }
    directives = Array.isArray(directives) ? directives.join('\n') : directives;
    this.props.updateAllProperties({
      workspaceId,
      directives,
      schema,
    });
    this.closeDataprepModal();
  };

  public onCodeEditorChange = (value) => {
    value = Array.isArray(value) ? value.join('\n') : value;
    this.props.onChange(value);
  };

  public render() {
    const { classes, widgetProps, disabled, extraConfig } = this.props;
    const properties = objectQuery(extraConfig, 'properties') || {};
    const placeholder = objectQuery(widgetProps, 'placeholder');
    let rows = objectQuery(widgetProps, 'rows');

    if (typeof rows === 'string') {
      rows = parseInt(rows, 10);
    }

    return (
      <React.Fragment>
        <div className={classes.root}>
          <If condition={this.state.reloadCodeEditor}>
            <LoadingSVG />
          </If>
          <If condition={!this.state.reloadCodeEditor}>
            <CodeEditor
              value={this.props.value}
              onChange={this.onCodeEditorChange}
              placeholder={placeholder}
              rows={rows}
              disabled={disabled}
            />
          </If>
          <Button
            className={classes.wrangleButton}
            variant="contained"
            color="primary"
            onClick={this.toggleDataprepModal}
            disabled={disabled}
          >
            Wrangle
          </Button>
        </div>
        <If condition={this.state.showDataprepModal}>
          <Modal
            isOpen={this.state.showDataprepModal}
            toggle={this.toggleDataprepModal}
            size="lg"
            modalClassName="wrangler-modal"
            backdrop="static"
            zIndex="1061"
          >
            <div className="modal-header">
              <h5 className="modal-title">Wrangle</h5>
              <button className={classes.modalBtnClose} onClick={this.closeDataprepModal}>
                <IconSVG name="icon-close" />
              </button>
            </div>
            <ModalBody>
              <DataPrepHome
                singleWorkspaceMode={true}
                workspaceId={properties.workspaceId}
                onSubmit={this.updateDirectivesAndCloseModal}
                enableRouting={false}
                disabled={disabled}
              />
            </ModalBody>
          </Modal>
        </If>
      </React.Fragment>
    );
  }
}

const StyledWranglerEditor = withStyles(styles)(WranglerEditor);

export default function WranglerEditorWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledWranglerEditor {...props} />
    </ThemeWrapper>
  );
}

(WranglerEditorWrapper as any).propTypes = {
  value: PropTypes.string,
  config: PropTypes.object,
  properties: PropTypes.object,
  disabled: PropTypes.bool,
  onChange: PropTypes.func,
};

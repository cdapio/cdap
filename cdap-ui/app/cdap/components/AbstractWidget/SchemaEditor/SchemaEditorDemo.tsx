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
import FormControl from '@material-ui/core/FormControl';
import { SchemaEditor } from 'components/AbstractWidget/SchemaEditor';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import FileDnD from 'components/FileDnD';
import { Button } from '@material-ui/core';
import { getDefaultEmptyAvroSchema } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import LoadingSVG from 'components/LoadingSVG';

const emptySchema = getDefaultEmptyAvroSchema();

const styles = (): StyleRules => {
  return {
    container: {
      height: 'auto',
      display: 'grid',
      gridTemplateColumns: '50% 50%',
      padding: '10px',
    },
    contentContainer: {
      height: '100%',
      display: 'grid',
    },
    btnContainer: {
      margin: '10px 0',
      textAlign: 'right',
    },
    btns: {
      marginLeft: '5px',
    },
  };
};

interface ISchemaEditorDemoBaseProps extends WithStyles<typeof styles> {}

class SchemaEditorDemoBase extends React.Component<ISchemaEditorDemoBaseProps> {
  public state = {
    loading: false,
    file: {},
    error: null,
    schema: emptySchema,
  };
  public modifiedSchema = emptySchema;
  public onDropHandler = (e) => {
    const reader = new FileReader();
    reader.onload = (evt) => {
      try {
        let schema = JSON.parse(evt.target.result as any);
        if (Array.isArray(schema)) {
          schema = schema[0];
        }
        this.setState(
          {
            schema,
            file: e[0],
            error: null,
            loading: true,
          },
          () => {
            setTimeout(
              () =>
                this.setState({
                  loading: false,
                }),
              1000
            );
          }
        );
      } catch (e) {
        this.setState({ error: e.message, loading: false });
      }
    };
    reader.readAsText(e[0], 'UTF-8');
  };

  public onExport = () => {
    const blob = new Blob([JSON.stringify(this.modifiedSchema, null, 4)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const exportFileName = 'schema';
    const a = document.createElement('a');
    a.href = url;
    a.download = `${exportFileName}.json`;

    const clickHandler = (event) => {
      event.stopPropagation();
    };
    a.addEventListener('click', clickHandler, false);
    a.click();
  };

  public clearSchema = () => {
    this.setState(
      {
        loading: true,
      },
      () => {
        setTimeout(() => {
          this.setState({
            loading: false,
            file: {},
            error: null,
            schema: emptySchema,
          });
        }, 500);
      }
    );
  };

  public render() {
    const { classes } = this.props;
    return (
      <div className={classes.container}>
        <FormControl component="fieldset" disabled={this.state.loading}>
          <div>
            <FileDnD
              onDropHandler={this.onDropHandler}
              file={this.state.file}
              error={this.state.error}
            />
            <div className={classes.btnContainer}>
              <Button
                className={classes.btns}
                onClick={this.onExport}
                variant="contained"
                color="primary"
              >
                Export
              </Button>
              <Button
                className={classes.btns}
                onClick={this.clearSchema}
                variant="contained"
                color="secondary"
              >
                Clear
              </Button>
            </div>
          </div>
        </FormControl>
        <div className={classes.contentContainer}>
          <If condition={this.state.loading}>
            <LoadingSVG />
          </If>
          <If condition={!this.state.loading}>
            <SchemaEditor
              schema={this.state.schema}
              onChange={({ tree: t, flat: f, avroSchema }) => {
                this.modifiedSchema = avroSchema;
              }}
            />
          </If>
        </div>
      </div>
    );
  }
}
const SchemaEditorDemo = withStyles(styles)(SchemaEditorDemoBase);
export default React.memo(SchemaEditorDemo);

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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Heading, { HeadingTypes } from 'components/Heading';
import { buildUrl } from 'services/resource-helper';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { Link } from 'react-router-dom';

const styles = (theme): StyleRules => {
  return {
    root: {
      borderRadius: '6px',
      width: '245px',
      border: `2px solid ${theme.palette.grey[500]}`,
      margin: '12px',
      minHeight: '250px',
    },
    imageContainer: {
      height: '107px',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      background: `linear-gradient(${theme.palette.white[50]}, ${theme.palette.grey[600]})`,
      borderTopRightRadius: '6px',
      borderTopLeftRadius: '6px',
    },
    image: {
      maxHeight: '60px',
    },
    title: {
      marginBottom: '10px',
    },
    body: {
      padding: '10px 15px',
      height: 'calc(100% - 107px - 48px)',
    },
    description: {
      color: theme.palette.grey[100],
      lineHeight: '1.2',
    },
    footer: {
      margin: '0 10px',
      borderTop: `1px solid ${theme.palette.grey[500]}`,
      padding: '10px 30px 18px',
      display: 'flex',
      justifyContent: 'center',
    },
    link: {
      width: '50%',
      '&:last-child:not(:only-child)': {
        textAlign: 'right',
      },
      '&:only-child': {
        textAlign: 'center',
      },
    },
  };
};

interface ILink {
  label: string;
  url: string;
}

interface IActionCardProps extends WithStyles<typeof styles> {
  config: {
    img: string;
    title: string;
    description: string;
    links: ILink[];
  };
}

const ActionCardView: React.FC<IActionCardProps> = ({ config, classes }) => {
  return (
    <div className={classes.root}>
      <div className={classes.imageContainer}>
        <img src={config.img} className={classes.image} />
      </div>
      <div className={classes.body}>
        <Heading type={HeadingTypes.h3} label={config.title} className={classes.title} />
        <div className={classes.description}>{config.description}</div>
      </div>
      <div className={classes.footer}>
        {config.links.slice(0, 2).map((link) => {
          const linkPrefix = '/cdap';
          const isNativeLink = !link.url.startsWith(linkPrefix);

          let url = link.url.includes(':')
            ? buildUrl(link.url, { namespace: getCurrentNamespace() })
            : link.url;

          if (isNativeLink) {
            return (
              <a href={url} key={link.label} className={classes.link}>
                {link.label}
              </a>
            );
          } else {
            url = url.slice(linkPrefix.length);
            return (
              <Link to={url} key={link.label} className={classes.link}>
                {link.label}
              </Link>
            );
          }
        })}
      </div>
    </div>
  );
};

const ActionCard = withStyles(styles)(ActionCardView);
export default ActionCard;

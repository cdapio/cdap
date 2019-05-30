import React from 'react';
import { Consumer } from '../FllContext';

interface IHeaderProps {
  type: string;
  first: number;
  total: number;
}

function Header({ type, first, total }: IHeaderProps) {
  return (
    <Consumer>
      {({ numTables, target }) => {
        let last;
        if (type === ('impact' || 'cause')) {
          last = first + numTables - 1 <= total ? first + numTables - 1 : total;
        } else {
          last = total;
        }

        const header =
          type === 'target'
            ? 'Select a field to view lineage and operations'
            : `Datasets used as ${type} by ${target}`;
        const subheaderRange = `${first} to ${last}`;
        const units = type === 'target' ? 'fields' : 'datasets';

        return (
          <div className={`${type} header`}>
            <div className="main-header">{header}</div>
            <div>{`Viewing ${subheaderRange} of ${total} ${units}`}</div>
          </div>
        );
      }}
    </Consumer>
  );
}

export default Header;

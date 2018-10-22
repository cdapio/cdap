import React from 'react';
import { render } from 'react-testing-library';
import Page500 from 'components/500';

describe('Page500', () => {
  it('renders correctly', () => {
    const { container } = render(<Page500 />);
    expect(container.firstChild).toMatchSnapshot();
  });
});


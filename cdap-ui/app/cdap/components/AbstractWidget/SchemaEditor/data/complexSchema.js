const complex1 = [
  {
    name: 'etlSchemaBody',
    schema: {
      type: 'record',
      name: 'etlSchemaBody',
      fields: [
        {
          name: 'name',
          type: 'string',
        },
        {
          name: 'email',
          type: 'string',
        },
        {
          name: 'phone',
          type: ['string', 'null'],
        },
        {
          name: 'age',
          type: ['string', 'null'],
        },
        {
          name: 'arr',
          type: {
            type: 'array',
            items: ['string', 'null'],
          },
        },
        {
          name: 'map1',
          type: [
            {
              type: 'map',
              keys: ['long'],
              values: ['double'],
            },
            'null',
          ],
        },
        {
          name: 'something',
          type: [
            {
              type: 'map',
              keys: ['string'],
              values: ['int'],
            },
            'string',
          ],
        },
        {
          name: 'enum1',
          type: [
            'null',
            {
              type: 'enum',
              symbols: ['something', 'somethingelse', 'nothing', 'maybesomething'],
            },
          ],
        },
        {
          name: 'arr5',
          type: [
            {
              type: 'array',
              items: [
                {
                  type: 'record',
                  name: 'a6a50504dc85041ba8c60fb16d578be37',
                  fields: [
                    {
                      name: 'rec1',
                      type: ['string', 'null'],
                    },
                    {
                      name: 'rec2',
                      type: ['string', 'null'],
                    },
                    {
                      name: 'rec3',
                      type: ['string', 'null'],
                    },
                  ],
                },
                'null',
              ],
            },
            'null',
          ],
        },
      ],
    },
  },
];

const complex2 = [
  {
    name: 'etlSchemaBody',
    schema: {
      type: 'record',
      name: 'etlSchemaBody',
      fields: [
        {
          name: 'arr5',
          type: [
            {
              type: 'array',
              items: [
                {
                  type: 'record',
                  name: 'a6a50504dc85041ba8c60fb16d578be37',
                  fields: [
                    {
                      name: 'rec1',
                      type: ['string', 'null'],
                    },
                    {
                      name: 'rec2',
                      type: ['string', 'null'],
                    },
                    {
                      name: 'rec3',
                      type: ['string', 'null'],
                    },
                  ],
                },
                'null',
              ],
            },
            'null',
          ],
        },
      ],
    },
  },
];
export { complex1, complex2 };

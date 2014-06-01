
::

                     Internet
                     │
                    ┌┴───────┐
                    │α       ├┬───┬┬─── DMZ
                    └┬┬─────┬┘│   │└[out of scope servers]
                     ││     │ │ ┌─│────┐
    test ─┬──────────┘│     │ │ │δ│    │
          └[potential │     │┌┴┐│┌┴┐┌─┐│
            looneys]  │     ││ω│││ ││ ││
                      │     │└┬┘│└─┘└┬┘│
                      │     │ │ └─┬──│─┘
    unmanaged ──────┬─┴╖   ╓┴─┴┬┬─┴──┴──── managed
                    │  ║   ║   ││
         [unmanaged │  ║   ║   │└[managed clients]
            clients]┘  ║   ║   │
                       ║VPN║   └[internal, non-
             wireless ─╨─┬─╜     critical servers]
                         ┊802.11
                         └[wireless clients]


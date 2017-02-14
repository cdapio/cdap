cdap CHANGELOG
==============

v2.28.4 (Feb 16, 2017)
----------------------
- Switch to using Chef::VERSION for cookbook restrictions ( Issue #207 )
- Set testing log level to error ( Issue #208 )
- Wrap all node attributes in version check ( Issue #209 )
- Restrict build-essential on Chef < 12.5 ( Issue: #211 )

v2.28.3 (Feb 9, 2017)
---------------------
- Set Node.js version for SDK 4.0+ ( Issue #204 )
- Add guard to prevent error on insecure clusters ( Issue #205 )

v2.28.2 (Feb 2, 2017)
---------------------
- Install rkerberos gem during init ( Issue: #201 )
- Move HBase grant commands to init ( Issue: #202 )

v2.28.1 (Jan 26, 2017)
----------------------
- Properly constrain version of base package installed with CLI ( Issue: #198 )
- Add CDAP 4.0.1, 3.5.3, and Ambari Service 4.0.2 ( Issue: #199 )

v2.28.0 (Jan 19, 2017)
----------------------
- Use krb5_principal and krb5_keytab LWRPs ( Issue: #181 )
- Add 3.5.2 and 4.0.0 and make 4.0.0 default ( Issues: #194 #195 )

v2.27.0 (Dec 14, 2016)
----------------------
- Do not set kafka.server.log.dirs so kafka.log.dir can be used ( Issue: #187 )
- Sync script with CDAP init scripts ( Issue: #188 )
- Include Java in CLI recipe, unless skip is requested ( Issue: #189 )
- Support HDP 2.5.3.0 ( Issue: #190 )
- Use precise repo for up through xenial ( Issue: #191 )
- Restrict ark cookbook version ( Issue: #192 )

v2.26.2 (Nov 10, 2016)
----------------------
- Always run security recipe in fullstack ( Issue: #185 )

v2.26.1 (Oct 25, 2016)
----------------------
- Add HDP 2.2.6.3 and 2.4.3.0 support ( Issue: #180 )
- Remove sticky bit from user JHS directories ( Issue: #182 )
- Honor the CDAP_USER environment ( Issue: #183 )

v2.26.0 (Oct 18, 2016)
----------------------
- Remove some settings from cdap-defaults.xml in CDAP ( Issue: #172 )
- Refactor for less code duplication ( Issue: #173 )
- Increase mass threshold ( Issue: #174 )
- Update README.md for CPCP ( Issue: #175 )
- Disable Style/FrozenStringLiteralComment cop ( Issue: #176 )
- Support HDP 2.3.6.0 ( Issue: #177 )
- Use CDAP 3.6.0 release ( Issue: #178 )

v2.25.1 (Sep 27, 2016)
----------------------
- Fix Chef supermarket package ( Issue #171 )

v2.25.0 (Sep 26, 2016)
----------------------
- Setup codeclimate and DRY up code ( Issue: #166 )
- Separate realm file into its own recipe ( Issues: #167 #168)
- Support CDAP SDK 3.5.1 ( Issue: #169 )

v2.24.1 (Sep 23, 2016)
----------------------
- Support CDAP 4.0 bash script ( Issue: #163 )

v2.24.0 (Sep 14, 2016)
----------------------
- Use latest released cdap-ambari-service version ( Issue: #157 )
- Rename LICENSE to LICENSE.txt ( Issue: #160 )
- Support IBM Open Platform (IOP) (Issue: #161 )

v2.23.2 (Sep 8, 2016)
---------------------
- Revert changes to init scripts until caskdata/cdap#6574 is merged ( Issue: #158 )

v2.23.1 (Sep 1, 2016)
---------------------
- Fix Router path ( Issue: #156 )

v2.23.0 (Aug 26, 2016)
----------------------
- Switch Ambari dependency to recommends ( Issue: #150 )
- Support new cdap script for CDAP 4.0+ ( Issue: #151 )
- Add checksum for CDAP 3.3.7 SDK ( Issue: #152 )
- Use CDAP 3.5 by default ( Issue: #153 )

v2.22.0 (Jul 26, 2016)
----------------------
- Support CDAP SDK 3.3.5 ( Issue: #140 )
- Support cdap-ambari-service installation on Ambari ( Issue: #141 )
- Update README to reflect current recipes and usage ( Issue: #142 )
- Support CDAP SDK 3.4.3 ( Issue: #143 )
- Set default CDAP version to 3.4.3-1 ( Issue: #144 )
- Update test kitchen ( Issue: #146 )
- Support CDAP SDK 3.3.6 ( Issue: #147 )

v2.21.1 (Jun 29, 2016)
----------------------
- Add checksum for CDAP 3.4.2 SDK ( Issue: #136 )
- Only restart SDK if actions include start ( Issue: #137 )
- Rewrite distribution to Precise ( Issues: #138 [COOK-102](https://issues.cask.co/browse/COOK-102) )

v2.21.0 (May 25, 2016)
----------------------
- Add CDAP SDK bin to PATH ( Issues: #133 [COOK-98](https://issues.cask.co/browse/COOK-98) )
- Set CDAP 3.4.1 default and support 3.3.4 SDK installs ( Issue: #134 )

v2.20.0 (May 10, 2016)
----------------------
- Do not install Node.js on CDAP 3.4+ ( Issue: #128 )
- Setting `security.server.ssl.enabled` should set `ssl.enabled` ( Issues: #129 [COOK-74](https://issues.cask.co/browse/COOK-74) )
- Update SDK hashes and default to CDAP 3.4.0 ( Issue: #131 )

Previous release CHANGELOGs are available on the project's [GitHub Releases](https://github.com/caskdata/cdap_cookbook/releases).

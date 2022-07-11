<?xml version="1.0" ?>
<xsl:transform
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        version="1.0">
  <!--
      Copyright © 2014-2022 Cask Data, Inc.

      Licensed under the Apache License, Version 2.0 (the "License"); you may not
      use this file except in compliance with the License. You may obtain a copy of
      the License at

      http://www.apache.org/licenses/LICENSE-2.0

      Unless required by applicable law or agreed to in writing, software
      distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
      WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
      License for the specific language governing permissions and limitations under
      the License.
    -->
  <!-- output should be in xml format -->
  <xsl:output method="xml" indent="yes"/>
  <!-- config-path parameter with file path for additional properties -->
  <xsl:param name="config-path"/>
  <xsl:template match="*">
    <!-- New line after xml header -->
    <xsl:text>&#x0A;</xsl:text>
    <!-- Specify the processing instruction and point to configuration.xsl-->
    <xsl:processing-instruction name="xml-stylesheet">type="text/xsl" href="configuration.xsl"</xsl:processing-instruction>
    <!-- Add a new line -->
    <xsl:text>&#x0A;</xsl:text>
    <!-- Add comment with copyright -->
    <xsl:comment>
      Copyright © 2014-2022 Cask Data, Inc.

      Licensed under the Apache License, Version 2.0 (the "License"); you may not
      use this file except in compliance with the License. You may obtain a copy of
      the License at

      http://www.apache.org/licenses/LICENSE-2.0

      Unless required by applicable law or agreed to in writing, software
      distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
      WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
      License for the specific language governing permissions and limitations under
      the License.
    </xsl:comment>
    <!-- Add a new line -->
    <xsl:text>&#x0A;</xsl:text>
    <xsl:copy>
      <!-- Copy all the configurations from existing cdap-default.xml -->
      <xsl:copy-of select="document('src/main/resources/cdap-default.xml')/configuration/*"/>
      <!-- Copy all the additional configurations -->
      <xsl:copy-of select="document($config-path)/configuration/*"/>
    </xsl:copy>
  </xsl:template>
</xsl:transform>
FROM gcr.io/cloud-data-fusion-images/cloud-data-fusion:latest
# For OSS CDAP, use "FROM gcr.io/cdapio/cdap:latest"

RUN rm -rf /opt/cdap/master/ext/runtimeproviders \
 && rm -rf /opt/cdap/master/ext/runtimes \
 && rm -rf /opt/cdap/master/ext/environments \
 && rm -rf /opt/cdap/master/lib/io.cdap.cdap.cdap* \
 && rm -rf /opt/cdap/master/artifacts/spark2_2.11 \
 && rm -rf /opt/cdap/master/artifacts/spark3_2.12

COPY opt/cdap/master/lib /opt/cdap/master/lib
COPY opt/cdap/master/ext /opt/cdap/master/ext
COPY opt/cdap/master/artifacts/spark2_2.11/* /opt/cdap/master/artifacts/spark2_2.11/
COPY opt/cdap/master/artifacts/spark3_2.12/* /opt/cdap/master/artifacts/spark3_2.12/

RUN chmod -R 755 /opt/cdap

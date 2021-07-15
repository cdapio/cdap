FROM us.gcr.io/cloud-data-fusion-images/build/cloud-data-fusion/github:latest

RUN rm opt/cdap/master/lib/io.cdap.cdap*
COPY opt/cdap/master/lib /opt/cdap/master/lib
RUN rm -rf opt/cdap/master/ext/environments/k8s
COPY opt/cdap/master/ext/environments/k8s /opt/cdap/master/ext/environments/k8s

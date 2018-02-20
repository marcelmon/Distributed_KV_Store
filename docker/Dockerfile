FROM sunithar/oracle-java8-ant

# Params:
WORKDIR /home/dependencies
ADD . /home/dependencies
EXPOSE 2181

# Install and configure dependencies:
RUN apt-get update
RUN apt-get install -y wget 
RUN wget http://apache.forsale.plus/zookeeper/zookeeper-3.4.11/zookeeper-3.4.11.tar.gz
RUN tar -xzf zookeeper-3.4.11.tar.gz
RUN echo "tickTime=2000" > ./zookeeper-3.4.11/conf/zoo.cfg
RUN echo "dataDir=/app" >> ./zookeeper-3.4.11/conf/zoo.cfg
RUN echo "clientPort=2181" >> ./zookeeper-3.4.11/conf/zoo.cfg
RUN cp ./zookeeper /etc/init.d/
RUN update-rc.d zookeeper defaults

WORKDIR /home/proj

# Set up script:
#CMD ./run.sh

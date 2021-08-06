
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Dockerfile for installing the necessary dependencies for building livy.
# See BUILDING.txt

#####
# Use ubuntu:focal base image to facilitate environment configuration.
#####
FROM ubuntu:focal

WORKDIR /root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

#####
# Disable suggests/recommends
#####
RUN echo APT::Install-Recommends "0"\; > /etc/apt/apt.conf.d/10disableextras
RUN echo APT::Install-Suggests "0"\; >>  /etc/apt/apt.conf.d/10disableextras

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true

#####
# Language and Encoding
#####
ENV LANG en_US:UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL C.UTF-8

######
# Install common dependencies from packages. Versions here are either
# sufficient or irrelevant.
#
# WARNING: DO NOT PUT JAVA APPS HERE! Otherwise they will install default
# Ubuntu Java.  See Java section below!
######
RUN apt-get -q update \
  && apt-get -q install -y --no-install-recommends \
  apt-utils zlib1g-dev unzip xz-utils dpkg-dev curl \
  g++ gcc make pkg-config git vim-tiny libssl-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# install R3.x
RUN apt-get -q update \
  && apt-get -q install -y --no-install-recommends \
  r-base \
  && apt-get clean  \
  && rm -rf /var/lib/apt/lists/*

#######
# OpenJDK 8
#######
RUN apt-get -q update \
  && apt-get -q install -y --no-install-recommends openjdk-8-jdk \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

######
# Install Apache Maven 3.6.3
######
ARG MAVEN_VERSION="3.6.3"
ARG USER_HOME_DIR="/root"
ARG BASE_URL="https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries"

RUN mkdir -p /usr/share/maven \
  && curl -Lso  /tmp/maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && tar -xzC /usr/share/maven --strip-components=1 -f /tmp/maven.tar.gz \
  && rm -v /tmp/maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "${USER_HOME_DIR}/.m2"

# install python2.7 and python3.8
ARG PYTHON_VERSION="3.8.2"

RUN mkdir -p /usr/share/python \
  && curl -Lso /tmp/python.tgz "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz" \
  && tar -xzC /usr/share/python --strip-components=1 -f /tmp/python.tgz \
  && rm -v /tmp/python.tgz \
  && cd /usr/share/python \
  && gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)" \
  && ./configure \
      --build="$gnuArch" \
      --enable-optimizations \
  && make -j "$(nproc)" \
  && make install \
  && rm -rf /usr/share/python \
  && find /usr/local -depth \
      \( \
        \( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
          -o \( -type f -a \( -name '*.pyc' -o -name '*.pyo' -o -name '*.a' \) \) \
          -o \( -type f -a -name 'wininst-*.exe' \) \
      \) -exec rm -rf '{}' +

RUN apt-get -q update \
  && apt-get -q install -y --no-install-recommends \
  python2.7 \
  && ln -s /usr/bin/python2.7 /usr/bin/python \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && find /usr/local -depth \
      \( \
        \( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
          -o \( -type f -a \( -name '*.pyc' -o -name '*.pyo' -o -name '*.a' \) \) \
          -o \( -type f -a -name 'wininst-*.exe' \) \
      \) -exec rm -rf '{}' +

######
# Default container setup working directory and command.
######
WORKDIR /usr/livy
CMD bash

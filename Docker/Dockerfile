#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM --platform=linux/arm64 ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    curl \
    gnupg \
    protobuf-compiler \
    rapidjson-dev \
    build-essential \
    openjdk-11-jdk \
    zip \
    unzip \
    clang-format \
    python3 \
    python3-pip

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
      BAZEL_BIN="bazel-6.0.0-linux-arm64"; \
    else \
      BAZEL_BIN="bazel-6.0.0-linux-x86_64"; \
    fi && \
    echo "Downloading $BAZEL_BIN for $ARCH" && \
    curl -L -o /usr/local/bin/bazel \
      "https://github.com/bazelbuild/bazel/releases/download/6.0.0/${BAZEL_BIN}" && \
    chmod +x /usr/local/bin/bazel

RUN echo "build --copt=-Ufdopen --host_copt=-Ufdopen" >> /etc/bazel.bazelrc
ENV BAZEL_SYSTEM_BAZELRC=/etc/bazel.bazelrc

WORKDIR /app

COPY . /app

RUN bazel --version

RUN bazel build @com_github_bazelbuild_buildtools//buildifier:buildifier
RUN bazel build service/tools/kv/api_tools/kv_service_tools

ENTRYPOINT ["./entrypoint.sh"]
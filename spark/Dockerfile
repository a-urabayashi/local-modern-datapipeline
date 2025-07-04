FROM bitnami/spark:3.4.1

USER root
# ===== 必要なライブラリ（Java & Python build用） =====
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev curl \
    libncursesw5-dev xz-utils tk-dev libxml2-dev \
    libxmlsec1-dev libffi-dev liblzma-dev git \
    net-tools procps \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app/scripts /app/jars \
    && ln -s /usr/lib/jvm/java-11-openjdk-arm64 /usr/lib/jvm/java-11-openjdk-amd64 \
    && ln -s /usr/lib/jvm/java-11-openjdk-arm64 /opt/bitnami/java 

# ===== Python 3.11のインストール =====
WORKDIR /usr/src
RUN wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz && \
    tar xzf Python-3.11.9.tgz && \
    cd Python-3.11.9 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall



RUN ln -sf /usr/local/bin/python3.11 /usr/bin/python && \
    ln -sf /usr/local/bin/python3.11 /usr/bin/python3 && \
    python3 -m ensurepip && \
    python3 -m pip install --upgrade pip && \
    pip install pyspark==3.4.1 delta-spark==2.4.0 boto3 \
        fastapi==0.104.1 uvicorn==0.24.0 pydantic==2.5.0 requests==2.31.0 \
        pandas==2.3.0 numpy==2.3.1

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN ln -s $JAVA_HOME /opt/bitnami/java || true

WORKDIR /app

# scriptsディレクトリをコピー
COPY scripts /app/scripts
RUN chmod +x /app/scripts/*.sh
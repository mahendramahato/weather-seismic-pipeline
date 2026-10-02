# official airflow image plus the AWS CLI (needed by phase4/sync_to_s3.sh)
# and the Amazon provider (GlueJobOperator)

FROM apache/airflow:3.3.2

# system packages need root download aws cli v2 for this machine cpu
# ($(uname -m) = aarch64 for apple install clean up
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip \
    && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscli.zip \
    && unzip -q /tmp/awscli.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscli.zip /var/lib/apt/lists/*

# python packages installed in airflow user
# pinning apache-airflow stops pip from changing Airflow's own version
# while installing the provider
USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" apache-airflow-providers-amazon

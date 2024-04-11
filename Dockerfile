FROM python:3.11.8-slim-bookworm as main
# set timezone
ENV TZ=Asia/Kolkata
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


RUN \
  ARCH=$(case ${TARGETPLATFORM:-linux/amd64} in \
  "linux/amd64")   echo "x86-64bit" ;; \
  "linux/arm64")   echo "aarch64"   ;; \
  *)               echo ""          ;; esac) && \
  echo "ARCH=$ARCH" && \
  # Install build dependencies
  apt-get update && \
  apt-get install -y curl build-essential unixodbc-dev g++ apt-transport-https && \
  curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg && \
  curl -sSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
  apt-get update && \
  # Install ODBC Driver for SQL Server
  ACCEPT_EULA='Y' apt-get install -y msodbcsql17  && \
  # Install dependencies (pyobdc)
  pip install --upgrade pip && \
  # Cleanup build dependencies
  apt-get remove -y curl apt-transport-https debconf-utils g++ gcc rsync unixodbc-dev build-essential gnupg2 && \
  apt-get autoremove -y && apt-get autoclean -y

# Use the lightweight base image
FROM main AS test
WORKDIR /home/code
COPY requirements.txt /home/code

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENTRYPOINT ["python3", "main.py"]
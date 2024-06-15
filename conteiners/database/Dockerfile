FROM debian:bullseye

# Set environment variables to reduce output from debconf
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    wget \
    gnupg \
    lsb-release \
    sudo \
    && rm -rf /var/lib/apt/lists/*

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-archive-keyring.gpg

RUN echo "deb [signed-by=/usr/share/keyrings/postgresql-archive-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt update && apt install -y \
    postgresql-16 \
    postgresql-client-16 \
    && rm -rf /var/lib/apt/lists/*

COPY startUpScript.sh /usr/local/bin/startUpScript.sh
RUN chmod +x /usr/local/bin/startUpScript.sh

CMD ["./startUpScript.sh"]
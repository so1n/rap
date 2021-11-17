FROM redis

RUN set -ex \
    && apt-get update \
    && apt-get install -y \
        wget \
    && mkdir /usr/local/etc/redis \
    && wget https://github.com/brandur/redis-cell/releases/download/v0.2.2/redis-cell-v0.2.2-x86_64-unknown-linux-gnu.tar.gz \
    && tar xvzf redis-cell-v0.2.2-x86_64-unknown-linux-gnu.tar.gz \
    && mv libredis_cell.so /usr/local/etc/redis/libredis_cell.so \
    && rm redis-cell-v0.2.2-x86_64-unknown-linux-gnu.tar.gz

CMD [ "redis-server", "--loadmodule /usr/local/etc/redis/libredis_cell.so" ]

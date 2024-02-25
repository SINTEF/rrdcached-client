FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    rrdcached \
    tini \
    && rm -rf /var/lib/apt/lists/*


RUN useradd -r -s /bin/false rrdcached && \
    mkdir -p /data/db /data/journal && \
    chown -R rrdcached:rrdcached /data && \
    mkdir -p /var/run/rrdcached && \
    chown -R rrdcached:rrdcached /var/run/rrdcached
USER rrdcached

EXPOSE 42217
WORKDIR /data
VOLUME [ "/data/db", "/data/journal" ]

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["/usr/bin/rrdcached","-g","-F", "-B", "-R", "-l", ":42217", "-p", "/var/run/rrdcached/rrdcached.pid", "-b", "/data/db", "-j", "/data/journal", "-U", "rrdcached", "-G", "rrdcached", "-w", "300", "-f", "3600", "-t", "4", "-V", "LOG_INFO"]
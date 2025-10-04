FROM mcr.microsoft.com/mssql-tools:latest
RUN apt-get update && apt-get install -y --no-install-recommends cron bash && rm -rf /var/lib/apt/lists/*

COPY scripts/mark_expired.sh /usr/local/bin/mark_expired.sh
RUN chmod +x /usr/local/bin/mark_expired.sh

# ...
RUN printf "SHELL=/bin/bash\n0 0 * * * root . /etc/environment; /usr/local/bin/mark_expired.sh >> /var/log/cron.log 2>&1\n" \
      > /etc/cron.d/expiry \
 && chmod 0644 /etc/cron.d/expiry \
 && crontab /etc/cron.d/expiry

CMD ["bash","-lc","touch /var/log/cron.log && cron -f"]

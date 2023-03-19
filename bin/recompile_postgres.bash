#!/usr/bin/env bash

set -e

cd ~/code/postgres/ || exit 1
make -j 8;
make install;
/opt/jc/postgresql/bin/pg_ctl restart -D /opt/jc/pgdata/ -l/tmp/jc_pg.log -o '-p 21800 -E -d 1 -c synchronous_commit=off -c log_statement=none -c wal_level=logical'
echo "done"
exit 0
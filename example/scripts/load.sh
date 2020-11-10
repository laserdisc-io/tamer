#!/usr/bin/env bash

now=$(date +%s)
sixtydaysago=$(($now - 60*24*60*60))

tamerload=$(basename $0)
TMPFILE=$(mktemp -t $tamerload) || exit 1

echo "CREATE TABLE IF NOT EXISTS users (id varchar(50) NOT NULL PRIMARY KEY, name varchar(50) NOT NULL, description varchar(50), modified_at timestamptz NOT NULL);" >> $TMPFILE

for ((ts=$sixtydaysago; ts<=$now; ts+=60*60)) # one entry every hour
do
  echo "INSERT INTO users VALUES('$ts', 'name_$ts', 'description_$ts', '$(date -r "$ts" +"%F %H:%m:%S").000Z');" >> $TMPFILE
done

echo "Created sample file $TMPFILE, running inserts in Postgres now..."
cat $TMPFILE | docker exec -i tamer-postgres psql -U postgres

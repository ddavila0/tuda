docker run --rm \
        --volume $(pwd)/monicrontab:/etc/cron.d/monicron \
        --volume $(pwd)/data:/aggregations \
        --volume $(pwd)/creds.json:/monicron/creds.json \
        --volume $(pwd)/ddavila.keytab:/monicron/ddavila.keytab \
        -p 5001-5300:5001-5300 \
	--hostname monicron.cern.ch \
	monicron:dev

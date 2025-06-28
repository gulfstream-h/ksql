seeker-build:
	go build -o ./cmd ksql
create-migration-dir:
	mkdir "ksqlmig"
seeker-create-migrating:
	cd ./ksqlmig/ && \
	../cmd/ksql create my_mig_table
seeker-up:
	cd ./ksqlmig/ && \
	../cmd/ksql up 1751067784_my_mig_table.sql --db_url=http://localhost:8088
seeker-down:
	cd ./ksqlmig/ && \
	../cmd/ksql down 1751067379_my_mig3.sql --db_url=http://localhost:8088

1751067379_my_mig3.sql:

1751067784_my_mig_table.sql:
seeker-build:
	go build -o ./cmd ksql
create-migration-dir:
	mkdir "ksqlmig"
seeker-create-migrating:
	cd ./ksqlmig/ && \
	../cmd/ksql create my_mig3
seeker-up:
	cd ./ksqlmig/ && \
	../cmd/ksql up 1750979312_my_mig3.sql --db_url=http://localhost:8088
seeker-down:
	cd ./ksqlmig/ && \
	../cmd/ksql down 1750979312_my_mig3.sql --db_url=http://localhost:8088
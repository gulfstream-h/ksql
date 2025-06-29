seeker-build:
	go build -o ./cmd ksql
create-migration-dir:
	mkdir "kmigrations"
seeker-create-migrating:
	cd ./kmigrations/ && \
	../cmd/ksql create my_mig_table
seeker-up:
	cd ./kmigrations/ && \
	../cmd/ksql up 1751067784_my_mig_table.sql --db_url=http://localhost:8088
seeker-down:
	cd ./kmigrations/ && \
	../cmd/ksql down 1751067379_my_mig3.sql --db_url=http://localhost:8088
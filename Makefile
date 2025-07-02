seeker-build:
	go build -o ./cmd ksql
create-migration-dir:
	cd ./example/migrations && \
	mkdir "migrations"
seeker-create-migrating:
	cd ./example/migrations/migrations && \
	../../../cmd/ksql create transactions
seeker-up:
	cd ./kmigrations/ && \
	../../../cmd/ksql up 1751478066_transactions.sql --db_url=http://localhost:8088
seeker-down:
	cd ./kmigrations/ && \
	../../../cmd/ksql down 1751478066_transactions.sql --db_url=http://localhost:8088
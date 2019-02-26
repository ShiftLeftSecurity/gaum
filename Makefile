PSQL ?= psql
export PGPASSWORD="mysecretpassword"

.PHONY: test-postgres
test-postgres-pgx: clean-docker
	docker run --name do_test_gaum -p 5469:5432 -e POSTGRES_PASSWORD=$(PGPASSWORD) -d postgres
	sleep 3
	PGPASSWORD=${PGPASSWORD} $(PSQL) -n -U postgres -h localhost -p 5469 -d postgres -w -f initial.sql
	go test ./db/postgres/.
	docker stop do_test_gaum
	docker rm do_test_gaum

test-postgres-pq: clean-docker
	docker run --name do_test_gaum -p 5469:5432 -e POSTGRES_PASSWORD=$(PGPASSWORD) -d postgres
	sleep 3
	PGPASSWORD=${PGPASSWORD} $(PSQL) -n -U postgres -h localhost -p 5469 -d postgres -w -f initial.sql
	go test ./db/postgrespq/.
	docker stop do_test_gaum
	docker rm do_test_gaum

.PHONY: clean-docker
clean-docker:
	docker stop do_test_gaum || true
	docker rm do_test_gaum || true

.PHONY: test-chain
test-chain:
	go test ./db/chain/.

.PHONY: test-all
test-all: test-chain test-postgres-pgx test-postgres-pq


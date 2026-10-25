.PHONY: build test up down logs go-build go-test

build:            ## build the Java server and client jars
	mvn -B -q package -DskipTests

test:             ## run Java unit tests
	mvn -B test

up:               ## start DynamoDB Local + the Pulse server
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f pulse-server

go-build:         ## build loadgen, worker and verify into ./bin
	cd go && go build -o ../bin/ ./cmd/...

go-test:
	cd go && go test ./...

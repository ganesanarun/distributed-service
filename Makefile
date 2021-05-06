compile:
	protoc api/v1/*.proto --go_out=. --proto_path=. --go_opt=paths=source_relative

test:
	go test -race ./...
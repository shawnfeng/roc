mkdir -p hello
mkdir -p goodbye
protoc hello.proto --go_out=plugins=grpc:hello
protoc goodbye.proto --go_out=plugins=grpc:goodbye
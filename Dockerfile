FROM golang

WORKDIR /go
COPY . .
RUN apt-get update
RUN apt-get install -y protobuf-compiler
RUN mkdir -p src/appsinstalled
RUN go get -u github.com/golang/protobuf/proto
RUN go get -u github.com/golang/protobuf/protoc-gen-go
RUN protoc --go_out=src/appsinstalled appsinstalled.proto
RUN go get -d -v ./...
RUN go install -v ./...
RUN go build /go/src/memcache_load/memcache_load.go

CMD ["memcache_load"]

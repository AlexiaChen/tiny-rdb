build:
	go build -v -o bin/tiny-rdb

test:
	go test -v -cover ./...

clean:
	rm -rf bin
	rm -f tiny-rdb
	rm -f *.db
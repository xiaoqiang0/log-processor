all:
	go build -o log-processor .
run:
	./log-processor

clean:
	rm -f log-processor log

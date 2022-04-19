cc=g++

main: main.o
	$(cc) -o main main.o
main.o: main.cpp struct.h
	$(cc) -c main.o main.cpp

.PHONY: clean
clean:
	-rm -rf *.o
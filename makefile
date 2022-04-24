cc=g++

yourSql: db.o
	$(cc) -o db db.o
db.o: main.cpp struct.h table.h
	$(cc) -c main.cpp -o db.o

.PHONY: clean
clean:
	-rm -rf *.o
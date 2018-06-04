all: words

words: main.cpp
	g++ -pthread -o words main.cpp -std=c++14 -lboost_system -lboost_filesystem
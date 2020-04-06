/*========================================================================
* Copyright (C) 2020 All rights reserved.
* Author: doubleQ
* File Name: FastReader.cpp
* Created Date: 2020-04-06
* Description:
 =======================================================================*/

#include <bits/stdc++.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

using namespace std;

const int num_thread = 32;

/* process float .csv file seperated by , here */
/* or you can modify the function for the specific file */
vector<vector<double>> thread_process(const char *start, const char *end){
    vector<vector<double>> lines;
    vector<double> line;
    double val = 0, frac = 1;
    bool is_neg = false;
    int j = 0;
    for(auto c = start; c < end; c++){
        if(*c == '\n'){
            line.push_back(is_neg? -val: val);
            lines.push_back(line);
            line.clear();
            j = 0;
            val = 0;
            is_neg = false;
            frac = 1;
        }
        else if(*c == ','){
            line.push_back(is_neg? -val: val);
            val = 0;
            is_neg = false;
            frac = 1;
        }
        else{
            if(*c == '-') is_neg = true;
            else if(*c == '.') frac = 0.1;
            else {
                val += frac * (*c - '0');
                if(frac < 1) frac *= 0.1;
            }
        }
    }
    return lines;
}

void magic_read(const string &file_name, bool &verbose){
    int fd = open(file_name.c_str(), O_RDONLY);
    int buf_size = lseek(fd, 0, SEEK_END);
    /* read file into buff using mmap */
    char *buf = (char *)mmap(NULL, buf_size, PROT_READ, MAP_SHARED, fd, 0);
    const size_t blockSize = buf_size / num_thread;
    char *last = buf;
    vector<future<vector<vector<double>>>> results;
    for (size_t i = 0; i < num_thread; i++) {
        const char *start = last + (i != 0);
        const char *end;
        if(i == num_thread-1) end = buf + buf_size;
        else{
            last = buf + blockSize + i * blockSize;
            /* find the nearest '\n' position when paritition */
            while(*last != '\n') last++;
            end = last;
        }
        if(verbose) cout << i << ": process buff (" << (start - buf) << ", " << (end - buf) << ")" << endl;
        /* process lines of block */
        auto result = async(launch::async, [start, end]() {return thread_process(start, end);});
        results.emplace_back(move(result));
    }

    /* combine part lines in each thread */
    vector<vector<double>> data;
    for (auto &futureResult : results) {
        auto result = futureResult.get();
        for(auto r: result) data.push_back(r);
    }

    // show head(10)
    if(verbose){
        for(int i = 0; i < 10 && i < data.size(); i++){
            for(int j = 0; j < 10 && j < data[i].size(); j++) 
                cout << data[i][j] << ",";
            cout << endl;
        }
    }
}

int main(){
    string f = "./test_200.txt";
    bool verbose = true;
    auto t1 = chrono::high_resolution_clock::now();
    magic_read(f, verbose);
    auto t2 = chrono::high_resolution_clock::now();
    cout << "Time cost " << chrono::duration <double, milli>(t2 - t1).count() << " ms" << endl;
}

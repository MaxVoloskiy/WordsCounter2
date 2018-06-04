#include <iostream>
#include <fstream>
#include <map>
#include <algorithm>
#include <vector>
#include <mutex>
#include <boost/algorithm/string.hpp>
#include <thread>
#include <condition_variable>
#include <atomic>

std::mutex qmutex;
std::mutex mapmutex;
std::condition_variable cv1;
std::condition_variable cv2;

inline std::chrono::high_resolution_clock::time_point get_current_time_fenced() {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto res_time = std::chrono::high_resolution_clock::now();
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return res_time;
}

template<class D>
inline long long to_us(const D &d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

class deQueue
{
private:
    std::deque<std::vector<std::string>> queue;
public:

    void push(std::vector<std::string> vector)
    {
        std::lock_guard<std::mutex> lock(mapmutex);
        queue.emplace_back(vector);
    }

    std::vector<std::string> pop()
    {
        if (!queue.empty())
        {
            auto value = queue.front();
            queue.pop_front();
            return value;
        }
        std::vector<std::string> vector;
        return vector;
    }

    bool empty()
    {
        return queue.empty();
    }
};


class mapMerging
{
private:
    std::deque<std::map<std::string, int>> maps_queue;
public:
    void merging(std::atomic_bool &atomic_bool, std::atomic_bool &cactive)
    {
        while (atomic_bool || !maps_queue.empty() || cactive)
        {
            std::unique_lock<std::mutex> lock(mapmutex);
            while ((maps_queue.empty()) || cactive) {
                cv2.wait(lock);
            }
            if (maps_queue.size() >= 2)
            {
                auto front1 = maps_queue.front();
                maps_queue.pop_front();
                auto front2 = maps_queue.front();
                maps_queue.pop_front();
                lock.unlock();

                auto itr = std::move(front2).begin();
                while(itr != std::move(front2).end()){
                    std::move(front1)[itr->first] += itr->second;
                    ++itr;
                }

                std::lock_guard<std::mutex> lock_guard(mapmutex);
                maps_queue.emplace_back(front1);
                cv2.notify_one();

            } else if (maps_queue.size() == 1 && cactive)
            {
                continue;
            } else return;
        }
    }

    void push(std::map<std::string, int> args)
    {
        std::lock_guard<std::mutex> lk(mapmutex);
        maps_queue.emplace_back(args);
        cv2.notify_one();
    }

    std::map<std::string, int> result()
    {
        return maps_queue.front();
    }
};

// Counting words
void counter(deQueue *queue, std::atomic_bool &running, std::atomic_bool &cactive,
             mapMerging *mapMerge)
{
    while (!queue->empty())
    {
        std::unique_lock<std::mutex> lk(qmutex);
        auto words = queue->pop();
        lk.unlock();

        std::map<std::string, int> new_map;
        for (auto &word : words)
        {
            word.erase(std::remove_if(word.begin(), word.end(), [](char c) { return !std::isalnum(c); }),
                    word.end());
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);
            new_map[word] += 1;
        }

        mapMerge->push(move(new_map));
        cv2.notify_one();
    }

    while (running)
    {
        std::unique_lock<std::mutex> lk(qmutex);

        while (queue->empty()) {
            cv1.wait(lk);
        }

        auto words = queue->pop();
        lk.unlock();
        std::map<std::string, int> new_map;

        for (auto &word : words)
        {
            word.erase(std::remove_if(word.begin(), word.end(), [](char c) { return !std::isalnum(c); }),
                    word.end());
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);
            new_map[word] += 1;
        }

        mapMerge->push(move(new_map));
        cv2.notify_one();
    }

    cactive = false;
}

int main(int argc, char *argv[])
{
    std::string filename = "data.txt";

    int portion = 50000;
    int counters = 2;
    int maps = 1;

    auto queue = new deQueue();
    auto mapMerge = new mapMerging();
    std::atomic<bool> atomic(true);
    std::atomic<bool> active(true);
    std::ifstream file(filename);

    std::vector<std::thread> cthreads;
    int j = 0;
    while (j < counters) {
        cthreads.emplace_back(counter, queue, ref(atomic), ref(active), mapMerge);
        ++j;
    }

    std::vector<std::thread> mthreads;
    j = 0;
    while (j < maps) {
        mthreads.emplace_back(&mapMerging::merging, mapMerge, ref(atomic), ref(active));
        ++j;
    }

    std::string string;
    int c = 0;
    std::vector<std::string> words_vector;

    auto start_time = get_current_time_fenced();

    while (file >> string)
    {
        words_vector.emplace_back(string);
        ++c;

        if (c == portion)
        {
            queue->push(move(words_vector));
            cv1.notify_one();
            words_vector.clear();
            c = 0;
        }
    }

    file.close();

    if (c != 0)
    {
        queue->push(move(words_vector));
        cv1.notify_one();
    }

    atomic = false;

    for (auto &th : cthreads) th.join();
    for (auto &th : mthreads) th.join();

    auto result = mapMerge->result();
    std::cout << "Result: " << result.size() << std::endl;
    std::cout << "Time: " <<  to_us(get_current_time_fenced() - start_time) / 1000000.0 << std::endl;

    std::ofstream f;
    f.open("result.txt");
    auto itr = result.begin();
    while (itr != result.end()) {
        f << itr->first << ": " << std::to_string(itr->second) << std::endl;
        ++itr;
    }
    f.close();
    return 0;

}

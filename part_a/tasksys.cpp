#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) : num_threads(num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

void thread_run(int task_num, IRunnable* runnable, int num_total_tasks, int num_workers) {
    for (int i = task_num; i < num_total_tasks; i += num_workers) {
        runnable->runTask(i, num_total_tasks);
    }
}

void thread_run_queue(IRunnable* runnable, int num_total_tasks, int* work_counter, std::mutex *m) {
    int work_idx;
    while (true) {
        (*m).lock();
        work_idx = ++(*work_counter);
        (*m).unlock();
        if (work_idx >= num_total_tasks) {
            return;
        }
        runnable->runTask(work_idx, num_total_tasks);
    }
}

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    threadpool = std::vector<std::thread>(num_threads);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    int num_workers = std::min(num_threads, num_total_tasks);
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(thread_run, i, runnable, num_total_tasks, num_workers);
    }

    // Join threads
    for (int i = 0; i < num_threads; i++) {
        threadpool[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */


void thread_run_queue_spin(IRunnable** runnable, int* num_total_tasks,
        std::mutex* sync_m, int* work_counter,
        std::mutex* done_m, int* done_counter,
        bool* finished,
        std::mutex* wait_mutex, int* waiting_threads) {

    int next_work_idx;
    while (true) {
        if (*finished) {
            break;
        }

        sync_m->lock();
        if ((*work_counter >= 0) && (*work_counter < *num_total_tasks)) {
            next_work_idx = (*work_counter)++;
            sync_m->unlock();

            (*runnable)->runTask(next_work_idx, *num_total_tasks);
            done_m->lock();
            (*done_counter)++;
            done_m->unlock();
        }
        else {
            sync_m->unlock();
        }

    }

    wait_mutex->lock();
    (*waiting_threads)--;
    wait_mutex->unlock();
}

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads),
    waiting_threads(num_threads) {
    threadpool = std::vector<std::thread>(num_threads);
    next_work_item = -1;
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(thread_run_queue_spin,
                &tasks, &num_tasks,
                &sync_mutex, &next_work_item,
                &done_mutex, &num_done_items,
                &finished,
                &waiting_mutex, &waiting_threads);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // indicate to threads that we're ready to terminate
    finished = true;

    // wait for threads to acknowledge termination flag
    while (waiting_threads > 0) {}

    for (int i = 0; i < num_threads; i++) {
        threadpool[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // set values that all threads are watching via pointers to these
    tasks = runnable;
    num_tasks = num_total_tasks;
    num_done_items = 0;

    // point threads at the start of the new queue, which starts them on the work queue
    sync_mutex.lock();
    next_work_item = 0;
    sync_mutex.unlock();

    // check if we've completed all items
    done_mutex.lock();
    while (num_done_items < num_total_tasks) {
        done_mutex.unlock();
        done_mutex.lock();
    }
    done_mutex.unlock();

    // set start of queue to invalid value so threads don't try to do work
    sync_mutex.lock();
    next_work_item = -1;
    sync_mutex.unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    next_work_id = 0;
    num_tasks = 0;  // skip immediately working
    num_waiting_threads = 0;
    threadpool = std::vector<std::thread>(num_threads);
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::run_sleeping, this, i);
    }

    // wait until all threads have acknowledged waiting
    while (num_waiting_threads < num_threads) {
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    finished = true;  // set exit condition
    cv.notify_all();
    for (int i = 0; i < num_threads; i++) {
        threadpool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run_sleeping(int thread_id) {
    int task;
    std::unique_lock<std::mutex> ul(m);
    ul.unlock();
    while (true) {
        // exit condition
        if (finished) {
            return;
        }

        m.lock();
        // do work if there is some to do
        if (next_work_id < num_tasks) {
            task = next_work_id++;
            m.unlock();
            tasks->runTask(task, num_tasks);
        } else {
            // indicate that we're waiting, notify main thread, and go to sleep
            num_waiting_threads++;
            master_mutex.lock();
            master_cv.notify_one();
            master_mutex.unlock();
            cv.wait(ul);  // TODO: why does this not result in deadlock if we have m locked here?
            m.unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    tasks = runnable;
    m.lock();
    next_work_id = 0;
    num_tasks = num_total_tasks;
    num_waiting_threads = 0;

    m.unlock();
    cv.notify_all();
    m.lock();

    if (num_waiting_threads == 0) {
        // all threads have work to do, so go to sleep until at least one thread is finished
        std::unique_lock<std::mutex> ul(master_mutex);
        m.unlock();
        master_cv.wait(ul);  // TODO: Why does this work if we've already locked master_mutex?
        ul.unlock();
    } else {
        m.unlock();
    }

    // wait for all threads to finish
    while (num_waiting_threads < num_threads) {
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

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

void workload_loop(IRunnable* runnable, int num_total_tasks, int* work_counter, std::mutex* m) {
    int work_idx;
    while (true) {
        m->lock();
        work_idx = ++(*work_counter);
        m->unlock();
        if (work_idx >= num_total_tasks) {
            return;
        }
        runnable->runTask(work_idx, num_total_tasks);
    }
}

void thread_run_queue_spin(IRunnable** runnable, int* num_total_tasks,
        std::mutex* sync_m, int* work_counter,
        std::mutex* start_m, bool* finished,
        std::mutex* wait_mutex, int* waiting_threads) {

    bool _all_done = true;
    while (true) {
        start_m->lock();
        _all_done  = *finished;
        start_m->unlock();

        if (_all_done) {
            wait_mutex->lock();
            (*waiting_threads)--;
            wait_mutex->unlock();
            return;
        }

        int first_work_idx;
        sync_m->lock();
        first_work_idx = *work_counter;
        sync_m->unlock();
        if (first_work_idx != -1 && first_work_idx < *num_total_tasks) {
            workload_loop(*runnable, *num_total_tasks, work_counter, sync_m);
        }
    }
}

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads),
    waiting_threads(num_threads) {
    threadpool = std::vector<std::thread>(num_threads);
    start_mutex.lock();
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(thread_run_queue_spin,
                &tasks, &num_tasks,
                &sync_mutex, &next_work_item,
                &start_mutex, &finished,
                &waiting_mutex, &waiting_threads);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // indicate to threads that we're ready to terminate
    // here, we already have `start_mutex` locked
    finished = true;
    start_mutex.unlock();

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
    finished = false;
    next_work_item = -1;
    start_mutex.unlock();  // let threads begin work on this

    // check if we've completed all items
    int counter;
    while (true) {
        sync_mutex.lock();
        counter = next_work_item;
        sync_mutex.unlock();
        if (counter >= num_total_tasks) {
            break;
        }
    }

    start_mutex.lock();
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
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

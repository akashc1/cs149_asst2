#include "tasksys.h"
#include <condition_variable>

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

void workload_loop(IRunnable** runnable, int* num_total_tasks,
        std::mutex* sync_m, int* work_counter,
        std::mutex* done_m, int* done_counter) {

    int next_work_idx;
    sync_m->lock();
    while (*work_counter >= 0 && *work_counter < *num_total_tasks) {
        next_work_idx = (*work_counter)++;
        sync_m->unlock();

        (*runnable)->runTask(next_work_idx, *num_total_tasks);
        done_m->lock();
        (*done_counter)++;
        done_m->unlock();

        sync_m->lock();
    }
    sync_m->unlock();
}

void thread_work_sleep(int thread_id, IRunnable** runnable, int* num_tasks,
        std::mutex* sync_m, int* work_counter,
        std::mutex* done_m, int* done_counter,
        std::mutex* continue_mutex, std::condition_variable* cv, int* ack_counter,
        std::mutex* waiting_mutex, bool* finish, int* waiting_threads,
        std::condition_variable* master_sleep,
        std::mutex* master_sleep_mutex, bool* master_awake) {

    while (true) {

        // wait until we're told we should continue
        std::unique_lock<std::mutex> ul(*continue_mutex);
        printf("[%d] Going to sleep until we are woken up\n", thread_id);
        cv->wait(ul);
        (*ack_counter)++;
        printf("[%d] Woken up, ack_counter=%d\n", thread_id, *ack_counter);
        ul.unlock();

        // exit condition
        if (*finish) {
            return;
        }

        // check if we should do work or not
        printf("[%d] Waiting for sync_mutex\n", thread_id);
        sync_m->lock();
        if ((*work_counter >= 0) && (*work_counter < *num_tasks)) {
            sync_m->unlock();
            workload_loop(runnable, num_tasks, sync_m, work_counter, done_m, done_counter);
        }
        else {
            sync_m->unlock();
        }

        // Wake up master thread
        if (thread_id == 0) {
            master_sleep_mutex->lock();
            while (!*master_awake) {
                master_sleep_mutex->unlock();
                master_sleep->notify_one();
                master_sleep_mutex->lock();
            }
            master_sleep_mutex->unlock();
            printf("[%d] Successfully woke up master\n", thread_id);
        }
    }
}

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    threadpool = std::vector<std::thread>(num_threads);
    continue_mutex.lock();
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(thread_work_sleep,
                i, &tasks, &num_tasks,
                &sync_mutex, &next_work_item,
                &done_mutex, &num_done_items,
                &continue_mutex, &cnt, &ack_counter,
                &waiting_mutex, &finished, &waiting_threads,
                &master_sleep, &master_sleep_mutex, &master_awake);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    printf("Calling destructor!\n");
    finished = true;

    ack_counter = 0;
    waiting_threads = 0;

    while (ack_counter < num_threads) {
        continue_mutex.unlock();
        cnt.notify_all();
        continue_mutex.lock();
    }

    for (int i = 0; i < num_threads; i++) {
        threadpool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    printf("[MASTER] Start run()\n");
    tasks = runnable;

    sync_mutex.lock();
    num_tasks = num_total_tasks;
    next_work_item = 0;
    num_done_items = 0;
    ack_counter = 0;

    // we already have continue_mutex locked here
    printf("[MASTER] Waking up all threads\n");
    while (ack_counter < num_threads) {
        continue_mutex.unlock();
        cnt.notify_all();
        continue_mutex.lock();
    }
    printf("[MASTER] All threads have acknowledged being awake\n");
    sync_mutex.unlock();

    // Go to sleep and wait to be notified by a thread that's completed its iteration
    std::unique_lock<std::mutex> master_lock(master_sleep_mutex);
    printf("[MASTER] Going to sleep until woken up by a thread\n");
    master_awake = false;
    master_sleep.wait(master_lock);
    master_awake = true;
    printf("[MASTER] Woken up\n");
    master_lock.unlock();

    done_mutex.lock();
    while (num_done_items < num_tasks) {
        done_mutex.unlock();
        done_mutex.lock();
    }
    done_mutex.unlock();
    next_work_item = -1;
    printf("[MASTER] Finishing call to run()\n");
    master_awake = false;
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

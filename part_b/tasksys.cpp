#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    threadpool = std::vector<std::thread>(num_threads);
    num_sleeping = 0;
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::work_from_queue, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    queue_lock.lock();
    finished = true;
    if (num_sleeping) {
        cv.notify_all();
    }
    queue_lock.unlock();

    for (int i = 0; i < num_threads; i++) {
        threadpool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> nodep;
    runAsyncWithDeps(runnable, num_total_tasks, nodep);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    queue_lock.lock();
    TaskID new_task_id = _runnables.size();

    _runnables.push_back(runnable);
    _all_num_tasks.push_back(num_total_tasks);
    _deps.push_back(deps);
    next_item.push_back(0);
    done_count.push_back(0);

    // if the task has no dependencies it's already ready to go
    if (deps.empty()) {
        ready_q.push_back(new_task_id);
    } else {
        // assume it's blocked (although it's possible the dependencies are already satisfied)
        blocked_q.push_back(new_task_id);
    }

    // wake up any sleeping threads so they can work on newly pushed things
    if (num_sleeping) {
        cv.notify_all();
    }
    queue_lock.unlock();

    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::work_from_queue() {
    int tid, item_num;
    std::unique_lock<std::mutex> ul(queue_lock);
    ul.unlock();
    while (true) {
        // we're done, exit
        queue_lock.lock();
        if (finished) {
            queue_lock.unlock();
            return;
        }

        // We have items that we can immediately work on
        if (!ready_q.empty()) {
            // get next ready task x item tuple
            tid = ready_q.front();
            item_num = next_item[tid]++;

            // remove this task from the queue if we're about to work on the last item from it
            if (item_num == _all_num_tasks[tid] - 1) {
                ready_q.pop_front();
            }
            queue_lock.unlock();

            // actually perform the task
            _runnables[tid]->runTask(item_num, _all_num_tasks[tid]);

            // publish that we've finished this item
            queue_lock.lock();
            done_count[tid]++;
            queue_lock.unlock();

        } else if (!blocked_q.empty()){
            // Here, all tasks in the queue are blocked due to dependencies. So we should
            // try to move items that have satisfied dependencies to the unblocked queue.
            for (unsigned int i = 0; i < blocked_q.size(); i++) {
                int blocked_tid = blocked_q[i];

                // check if this item's all unblocked
                bool all_deps_satisfied = true;
                for (unsigned int j = 0; j < _deps[blocked_tid].size(); j++) {
                    int depid = _deps[blocked_tid][j];
                    if (done_count[depid] != _all_num_tasks[depid]) {
                        all_deps_satisfied = false;
                        break;
                    }
                }

                // move from blocked queue to the ready queue
                if (all_deps_satisfied) {
                    ready_q.push_back(blocked_tid);
                    blocked_q.erase(blocked_q.begin() + i);
                    i--;  // push back iterator since erase moves items in the queue
                }
            }

            queue_lock.unlock();
        } else {
            // Both queues are empty, we can sleep until woken up at some point in the future
            // indicate that we're gonna go to sleep
            num_sleeping++;
            cv.wait(ul);
            num_sleeping--;
            queue_lock.unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    while (true) {
        bool all_done = true;
        queue_lock.lock();
        for (unsigned int i = 0; i < _runnables.size(); i++) {
            if (done_count[i] != _all_num_tasks[i]) {
                all_done = false;
                break;
            }
        }
        queue_lock.unlock();
        if (all_done) {
            break;
        }
    }

    return;
}

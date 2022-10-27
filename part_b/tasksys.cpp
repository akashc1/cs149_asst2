#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads): num_threads(num_threads) {}
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    threadpool = std::vector<std::thread>(num_threads);
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(
                &TaskSystemParallelThreadPoolSleeping::work_from_queue, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    queue_lock.lock();
    finished = true;
    while (num_sleeping) {
        queue_lock.unlock();
        cv.notify_all();
        queue_lock.lock();
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
    if (deps.size() == 0) {
        ready_q.push_back(new_task_id);
        /* printf("[MASTER] Pushing task_id=%d to the ready queue, no deps\n", new_task_id); */
    } else {
        // assume it's blocked (although it's possible the dependencies are already satisfied
        blocked_q.push_back(new_task_id);
        /* printf("[MASTER] Pushing task_id=%d to the blocked queue\n", new_task_id); */
    }

    // wake up any sleeping threads so they can work on newly pushed things
    if (num_sleeping) {
        cv.notify_all();
    }

    queue_lock.unlock();

    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::work_from_queue(int thread_id) {
    int tid, item_num;
    while (true) {
        queue_lock.lock();
        if (finished) {
            queue_lock.unlock();
            return;
        }

        // we have ready tasks to work on
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
            /* printf("[%d] Finished an item from task %d\n", thread_id, tid); */

        } else if (!blocked_q.empty()) {
            // Here, all tasks in the queue are blocked due to dependencies. So we should
            // try to move items that have satisfied dependencies to the unblocked queue.
            /* printf("[%d] Gonna try to rearrange tasks in queues\n", thread_id); */
            std::vector<int> unblocked_tasks;
            for (unsigned int i = 0; i < blocked_q.size(); i++) {
                int tid = blocked_q[i];
                bool unblocked = true;
                // iterate over the dependencies to see if all of them are satisfied
                for (unsigned int j = 0; j < _deps[tid].size(); j++) {
                    int dep_tid = _deps[tid][j];
                    if (done_count[dep_tid] != _all_num_tasks[dep_tid]) {
                        unblocked = false;
                        break;
                    }
                    /* printf("[%d] Task %d is fully completed: %d / %d\n", */
                    /*        thread_id, dep_tid, done_count[dep_tid], */
                    /*        _all_num_tasks[dep_tid]); */
                    if (unblocked) {
                        unblocked_tasks.push_back(i);
                    }
                }
            }

            // push any tasks with all dependencies satisfied to the unblocked queue
            for (unsigned int i = 0; i < unblocked_tasks.size(); i++) {
                ready_q.push_back(blocked_q[unblocked_tasks[i]]);
                int tid = blocked_q[unblocked_tasks[i]];
                /* printf( */
                /*     "[%d] Pushed task %d to the unblocked queue, done %d / %d\n", */
                /*     thread_id, tid, done_count[tid], _all_num_tasks[tid]); */
                blocked_q.erase(blocked_q.begin() + unblocked_tasks[i]);
            }
            queue_lock.unlock();
        } else {
            // Both queues are empty, we can sleep until woken up at some point in the future
            // indicate that we're gonna go to sleep
            /* printf("[%d] Gonna go to sleep...\n", thread_id); */
            num_sleeping++;
            queue_lock.unlock();
            std::unique_lock<std::mutex> ul(queue_lock);
            cv.wait(ul);
            num_sleeping--;
            ul.unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    while (true) {
        queue_lock.lock();
        bool all_done = true;
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

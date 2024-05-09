import ray
import time
import itertools

def parallel_loop(iterable, fct, n_tasks=4, return_results=False, init_and_shutdown_ray=True, use_object_store=False):
    """
    Executes a function `fct` in parallel across chunks of `iterable` using Ray.

    This function is designed to run independent iterations of a loop in parallel, 
    distributing the work across `n_tasks` tasks. It's crucial that the iterations 
    of the loop are independent to ensure correct execution and results.

    Parameters:
    - iterable (iterable): The iterable to process in parallel.
    - fct (function): The function to apply to each element of `iterable`. This function should be independent across iterations.
    - n_tasks (int, optional): The number of parallel tasks to use. Defaults to 4.
    - return_results (bool, optional): Flag to indicate whether to return the results of applying `fct` to the elements of `iterable`. Defaults to False.

    Returns:
    - list or None: If `return_results` is True, returns a list of the results of the function application. Otherwise, returns None.
    
    Note:
    - This function requires Ray to be initialized and will do so upon execution. Ray is shutdown after task completion.
    """

    if init_and_shutdown_ray:
        ray.init()

    n = len(iterable)
    chunk_size = max(n // n_tasks, 1)  # Ensuring at least 1 item per task

    @ray.remote
    def grouped_iterations_task(iterable, start_index=None, end_index=None, use_object_store=False):
        results = []
        # Fetch the actual iterable based on whether it's a reference or a direct list

        sub_iterable = iterable[start_index:end_index] if start_index is not None else iterable

        for el in sub_iterable:
            if isinstance(el, (list, tuple, set)):
                result = fct(*el)
            else:
                result = fct(el)
            if return_results:
                results.append(result)
        return results

    tasks = []
    for i in range(0, n, chunk_size):
        end_index = min(i + chunk_size, n)
        sub_iterable = iterable[i:end_index]
        task = grouped_iterations_task.remote(sub_iterable, use_object_store=False)
        tasks.append(task)

    results = ray.get(tasks)

    if init_and_shutdown_ray:
        ray.shutdown()

    if return_results:
        # Flatten the list of results if needed
        flatten_results = [item for sublist in results for item in sublist]
        return flatten_results
    else:
        return None
    

def parallel_loop_lazy(iterable, total_length, fct, n_tasks=4, return_results=False, init_and_shutdown_ray=True, use_object_store=False):
    """
    Executes a function `fct` in parallel across chunks of `iterable` using Ray.

    This function is designed to run independent iterations of a loop in parallel, 
    distributing the work across `n_tasks` tasks. It's crucial that the iterations 
    of the loop are independent to ensure correct execution and results.

    Parameters:
    - iterable (iterable): The iterable to process in parallel.
    - fct (function): The function to apply to each element of `iterable`. This function should be independent across iterations.
    - n_tasks (int, optional): The number of parallel tasks to use. Defaults to 4.
    - return_results (bool, optional): Flag to indicate whether to return the results of applying `fct` to the elements of `iterable`. Defaults to False.

    Returns:
    - list or None: If `return_results` is True, returns a list of the results of the function application. Otherwise, returns None.
    
    Note:
    - This function requires Ray to be initialized and will do so upon execution. Ray is shutdown after task completion.
    """

    if init_and_shutdown_ray:
            ray.init()

    # Initialize an iterator from the iterable
    it = iter(iterable)
    # Determine chunk size based on the total length and number of tasks

    chunk_size = max(total_length // n_tasks, 1)

    @ray.remote
    def process_chunk(chunk, return_results):
        results = []
        for elements in chunk:
            result = fct(*elements) if isinstance(elements, (list, tuple, set)) else fct(elements)
            if return_results:
                results.append(result)
        return results

    tasks = []
    for _ in range(n_tasks):
        # Create a chunk using islice, it will not materialize the full list
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            break  # No more data to process
        task = process_chunk.remote(chunk, return_results)
        tasks.append(task)

    results = ray.get(tasks)

    if init_and_shutdown_ray:
        ray.shutdown()

    if return_results:
        # Flatten the list of results if needed
        flatten_results = [item for sublist in results for item in sublist]
        return flatten_results
    else:
        return None




@ray.remote
class MonitoringActor:
    def __init__(self):
        self.states = {}  # Stores task_id and their latest completion percentage

    def update_state(self, task_id, completion_percentage):
        self.states[task_id] = completion_percentage

    def get_states(self):
        return self.states

    def get_average_completion(self):
        if not self.states:
            return 0
        total_completion = sum(self.states.values())
        average_completion = total_completion / len(self.states)
        return average_completion

@ray.remote
class TaskActor:
    def __init__(self, monitoring_actor):
        self.monitoring_actor = monitoring_actor

    def process_chunk(self, chunk, fct, task_id, return_results):
        results = []
        total_elements = len(chunk)
        for index, el in enumerate(chunk):
            completion_percentage = (index + 1) / total_elements * 100
            self.monitoring_actor.update_state.remote(task_id, completion_percentage)
            result = fct(*el) if isinstance(el, (list, tuple, set)) else fct(el)
            if return_results:
                results.append(result)
        return results

def parallel_loop_lazy_with_progress(iterable, total_length, fct, n_tasks=4, return_results=False, init_and_shutdown_ray=True, progress_update_interval=5):
    if init_and_shutdown_ray:
        ray.init()

    monitoring_actor = MonitoringActor.remote()  # Initialize the monitoring actor
    it = iter(iterable)
    chunk_size = max(total_length // n_tasks, 1)
    actors = [TaskActor.remote(monitoring_actor) for _ in range(n_tasks)]
    tasks = []

    # Schedule tasks with chunks
    for i, actor in enumerate(actors):
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            break
        task = actor.process_chunk.remote(chunk, fct, i, return_results)
        tasks.append(task)

    pending_tasks = list(tasks)
    while pending_tasks:
        completed_tasks, pending_tasks = ray.wait(pending_tasks, num_returns=1, timeout=2)
        average_completion = ray.get(monitoring_actor.get_average_completion.remote())
        print(f"Completion: {average_completion:.2f}%")
        time.sleep(progress_update_interval)

    if return_results:
        results = ray.get(tasks)
        flatten_results = [item for sublist in results for item in sublist]
        results = flatten_results

    if init_and_shutdown_ray:
        ray.shutdown()

    return results if return_results else None





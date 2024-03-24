import ray

def parallel_loop(iterable, fct, n_tasks=4, return_results=False):
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
    ray.init()

    # Calculate the size of each chunk to distribute tasks evenly
    n = len(iterable)
    chunk_size = max(n // n_tasks, 1)  # Ensuring at least 1 item per task

    @ray.remote
    def grouped_iterations_task(sub_iterable):
        results = []
        for el in sub_iterable:
            # Check if `el` is an iterable that should be unpacked
            if isinstance(el, (list, tuple, set)):
                result = fct(*el)
            else:
                # For single values or other non-tuple iterables, pass them as a single-item tuple
                result = fct(el)
            if return_results:
                results.append(result)
        return results

    tasks = []
    for i in range(0, n, chunk_size):
        # Adjust the end index to not exceed the iterable's length
        end_index = min(i + chunk_size, n)
        sub_iterable = iterable[i:end_index]
        task = grouped_iterations_task.remote(sub_iterable)
        tasks.append(task)

    results = ray.get(tasks)

    ray.shutdown()

    if return_results:
        # Flatten the list of results if needed
        flatten_results = [item for sublist in results for item in sublist]
        return flatten_results
    else:
        return None

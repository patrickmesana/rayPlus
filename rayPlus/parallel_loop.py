import ray

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

    if use_object_store:
        # Store the iterable in the Ray object store
        iterable_ref = ray.put(iterable)
    else:
        iterable_ref = iterable

    @ray.remote
    def grouped_iterations_task(iterable, start_index=None, end_index=None, use_object_store=False):
        results = []
        # Fetch the actual iterable based on whether it's a reference or a direct list
        if use_object_store:
            sub_iterable = iterable[start_index:end_index]
        else:
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
        # Pass indices and reference to the remote function if using object store, otherwise pass slices directly
        if use_object_store:
            task = grouped_iterations_task.remote(iterable_ref, i, end_index, use_object_store=True)
        else:
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

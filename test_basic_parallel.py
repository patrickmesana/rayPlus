#%%
n = 10003

def identity_fct(x1, x2):
   return x1 + x2

#%%

[identity_fct(*x) for x in list(enumerate(list(range(n))))]
    
    
# %%
from rayPlus import parallel_loop


# Warning! if return_results=True, the results will be stored in memory and it can be a problem if the results are too big, it will crash your computer
parallel_loop(list(enumerate(list(range(n)))), identity_fct, return_results=True, n_tasks=4)


# %%

# %%
from rayPlus import parallel_loop_lazy
parallel_loop_lazy(enumerate(list(range(n))), n , identity_fct, return_results=True, n_tasks=4)
# %%

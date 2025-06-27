import torch
import gc


gc.collect()
torch.cuda.empty_cache()
torch.cuda.ipc_collect()
import pandas as pd
import numpy as np
import torch
import random
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from deap import base, creator, tools, algorithms
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from tqdm import tqdm
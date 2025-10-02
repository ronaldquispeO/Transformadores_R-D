import pandas as pd
import numpy as np
from NUCrnuc import get_df_RNUC, get_df_extendida_RNUC
from NUCiex import get_df_IEX, get_df_extendida_IEX

from functools import reduce

# =============================
# PESOS DE LOS ÍNDICES
# =============================
pesos_NUC = {
    'ROHM': 3,
    'RTRA': 5,
    'ZCC': 5,
}
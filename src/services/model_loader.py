# # services/model_loader.py
# import joblib
# import os

# BASE_DIR = os.getcwd()  # change if needed

# PLSR_FAT_PATH = "/Users/kdn_aigayatrikadam/Documents/Projects/patners-squad/fastAPI/src/model/plsr_fat_model.pkl"
# PLSR_TS_PATH = "/Users/kdn_aigayatrikadam/Documents/Projects/patners-squad/fastAPI/src/model/plsr_total_solids_model.pkl"
# ISO_PATH = "/Users/kdn_aigayatrikadam/Documents/Projects/patners-squad/fastAPI/src/model/adulteration_isolation_forest.pkl"

# class ModelBundle:
#     def __init__(self):
#         self.pls_fat = None
#         self.pls_ts = None
#         self.iso = None
#         self._load_models()

#     def _load_models(self):
#         if os.path.exists(PLSR_FAT_PATH):
#             fat_bundle = joblib.load(PLSR_FAT_PATH)
#             self.pls_fat = fat_bundle  # dict with 'pls_model', 'spectral_cols', 'window'...
#         else:
#             raise FileNotFoundError(f"{PLSR_FAT_PATH} not found")

#         if os.path.exists(PLSR_TS_PATH):
#             ts_bundle = joblib.load(PLSR_TS_PATH)
#             self.pls_ts = ts_bundle
#         else:
#             raise FileNotFoundError(f"{PLSR_TS_PATH} not found")

#         if os.path.exists(ISO_PATH):
#             iso_bundle = joblib.load(ISO_PATH)
#             self.iso = iso_bundle
#         else:
#             # optional: not fatal if you plan to use mqtt-provided adulteration risk
#             self.iso = None

# models = ModelBundle()

import numpy as np
from scipy.signal import savgol_filter
import joblib
import os

PLSR_FAT_PATH = "/Users/kdn_aigayatrikadam/Documents/Projects/patners-squad/fastAPI/src/model/plsr_fat_model.pkl"
PLSR_TS_PATH = "/Users/kdn_aigayatrikadam/Documents/Projects/patners-squad/fastAPI/src/model/plsr_total_solids_model.pkl"
ISO_PATH = "/Users/kdn_aigayatrikadam/Documents/Projects/patners-squad/fastAPI/src/model/adulteration_isolation_forest.pkl"


# ------------------------------------------------------
# PREPROCESSING FUNCTIONS (same as training!)
# ------------------------------------------------------
def apply_savgol(spectra, window, poly, derivative):
    if window % 2 == 0:
        window += 1
    return savgol_filter(
        spectra,
        window_length=window,
        polyorder=poly,
        deriv=derivative,
        axis=1
    )


def apply_snv(spectra):
    return (spectra - spectra.mean(axis=1, keepdims=True)) / (
        spectra.std(axis=1, keepdims=True) + 1e-12
    )


def apply_minmax_norm(spectra):
    mn = spectra.min(axis=1, keepdims=True)
    mx = spectra.max(axis=1, keepdims=True)
    return (spectra - mn) / (mx - mn + 1e-12)


# ------------------------------------------------------
# ModelBundle loader
# ------------------------------------------------------
class ModelBundle:
    def __init__(self):
        self.pls_fat = None
        self.pls_ts = None
        self.iso = None
        self._load_models()

    def _load_models(self):

        self.pls_fat = joblib.load(PLSR_FAT_PATH)
        self.pls_ts = joblib.load(PLSR_TS_PATH)
        self.iso = joblib.load(ISO_PATH)


# expose everything
models = ModelBundle()

# Attach preprocessing functions so other modules can import through models
models.apply_savgol = apply_savgol
models.apply_snv = apply_snv
models.apply_minmax_norm = apply_minmax_norm

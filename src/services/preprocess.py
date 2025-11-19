# services/preprocess.py
import numpy as np
from scipy.signal import savgol_filter

def apply_savgol(spectra: np.ndarray, window=15, poly=3, derivative=1):
    # spectra shape: (n_samples, n_wavelengths) or (1, n_wavelengths)
    if window % 2 == 0:
        window += 1
    if window <= poly:
        raise ValueError(f"Window length ({window}) must be > polyorder ({poly}).")
    return savgol_filter(spectra, window_length=window, polyorder=poly, deriv=derivative, axis=1)

def apply_snv(spectra: np.ndarray):
    # Standard Normal Variate: row-wise mean/std normalization
    mean = spectra.mean(axis=1, keepdims=True)
    std = spectra.std(axis=1, keepdims=True)
    std[std == 0] = 1e-8
    return (spectra - mean) / std

def apply_minmax_norm(spectra: np.ndarray):
    mn = spectra.min(axis=1, keepdims=True)
    mx = spectra.max(axis=1, keepdims=True)
    denom = (mx - mn) + 1e-12
    return (spectra - mn) / denom

def preprocess_vector(absorbance_vector, window=15, poly=3, derivative=1):
    """
    absorbance_vector: 1D numpy array (n_wavelengths,)
    returns processed 1D numpy array of same length
    """
    arr = np.array(absorbance_vector, dtype=float).reshape(1, -1)
    sg = apply_savgol(arr, window=window, poly=poly, derivative=derivative)
    snv = apply_snv(sg)
    norm = apply_minmax_norm(snv)
    return norm.flatten()

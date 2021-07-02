"""Pytest config file."""
import matplotlib.pyplot as plt

# To avoid the following error: "RuntimeError: main thread is not in main loop"
plt.switch_backend("agg")

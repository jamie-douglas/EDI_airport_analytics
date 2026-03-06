
import time

def step(prev: float, msg: str) -> float:
    """
    Print progress with elapsed seconds.

    Parameters
    ----------
    prev : float
        Previous timestamp (time.perf_counter())
    msg : str
        Description of the step

    Returns
    -------
    float
        New timestamp
    """
    now = time.perf_counter()
    print(f"    ✓ {msg}   [{now - prev:0.2f}s]")
    return now

from celery import Celery
from src.ensemble_model import EnsembleModel
from multiprocessing import Pool

app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task
def run_ensemble(chunk):
    model = EnsembleModel()
    
    # Use multiprocessing Pool to process URLs in parallel
    with Pool() as pool:
        results = pool.map(model.process_single_url, chunk)
    
    return results

@app.task
def process_results(results):
    # Post-process results from multiple tasks
    pass

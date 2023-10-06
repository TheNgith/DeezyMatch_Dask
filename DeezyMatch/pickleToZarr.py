from os import path
import pickle
from dask import array as da

def pickleToZarr(directory: str):
    folder = path.dirname(directory)
    filename = path.basename(directory)
    
    data = pickle.load(open(path, 'rb'))
    
    da.to_zarr(data, folder + f'/{filename}.zarr')

from os import path
import pickle
from dask import array as da
from dask import dataframe as dd
from data_processing import lookupToken
import numpy as np

def pickleToZarr(directory: str):
    folder = path.dirname(directory)
    filename, ext = path.splitext(path.basename(directory))
    data = pickle.load(open(directory, 'rb'))
    data = da.from_array(np.array(data))
    da.to_zarr(data, folder + f'{filename}_updated{ext}')
    
def update_lookupToken(directory: str):
    folder = path.dirname(directory)
    filename, ext = path.splitext(path.basename(directory))
    data = pickle.load(open(directory, 'rb'))
    
    
    new_data = lookupToken(data.name)
    
    
    new_tok2index_dict = dict()
    for key in data.tok2index.keys():
        new_tok2index_dict[key] = [data.tok2index[key]]
    
    new_data.tok2index = dd.from_dict(new_tok2index_dict, npartitions=3)
    
    
    new_data.tok2count = data.tok2count
    
    
    new_index2tok_dict = dict()
    for key in data.index2tok.keys():
        new_index2tok_dict[str(key)] = [data.index2tok[key]]
    
    new_data.index2tok = dd.from_dict(new_index2tok_dict, npartitions=3)
    
    
    new_data.n_tok = data.n_tok
    
    
    pickle.dump(new_data, open(folder + f'{filename}_updated{ext}', 'wb'))
    

    
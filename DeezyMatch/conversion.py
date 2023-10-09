from os import path
from dask import array as da
from dask import dataframe as dd
from numpy import array as nparray

import pickle

from .data_processing import lookupToken

class pickleFailed(Exception): pass
class DaskArrayFailed(Exception): pass
class updatelookupTokenFailed(Exception): pass

def pickleToZarr(directory: str):
    folder = path.dirname(directory)
    filename, ext = path.splitext(path.basename(directory))
    
    try: data = pickle.load(open(directory, 'rb'))
    except Exception as e:
        raise pickleFailed(e)
    else:
        if not (data.__class__ is list):
            raise DaskArrayFailed
        try: 
            data = da.from_array(nparray(data))
        except Exception as e:
            raise DaskArrayFailed(e)
            
        new_path = folder + f'/{filename}_updated{ext}'
        da.to_zarr(data, new_path)
    
    return new_path
    
    
def update_lookupToken(data, directory: str):
    folder = path.dirname(directory)
    filename, ext = path.splitext(path.basename(directory))
    
    new_data = lookupToken(data.name)
    
    try:
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
        
        new_path = folder + f'/{filename}_updated{ext}'
    
    except Exception as e:
        raise updatelookupTokenFailed(e)
    
    else:
        pickle.dump(new_data, open(new_path, 'wb'))
        return new_data, new_path

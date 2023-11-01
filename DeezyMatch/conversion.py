from os import path
from dask import array as da
from dask import dataframe as dd
from numpy import array as nparray

import pickle

from .lookupToken_class import lookupToken

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
        
        new_data.tok2index = data.tok2index
        
        new_data.tok2count = data.tok2count
        
        new_data.index2tok = data.index2tok
        
        new_data.n_tok = data.n_tok
        
        new_path = folder + f'/{filename}_updated{ext}'
    
    except Exception as e:
        raise updatelookupTokenFailed(e)
    
    else:
        pickle.dump(new_data, open(new_path, 'wb'))
        return new_data, new_path

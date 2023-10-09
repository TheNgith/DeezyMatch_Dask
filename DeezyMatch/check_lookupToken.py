from dask import dataframe as da

def islookupToken(object):
    try: 
        object.tok2index
        object.index2tok
        object.tok2count
        object.n_tok
    except:
        return 0
    else: 
        if (object.tok2count.__class__ is dict) and (object.n_tok.__class__ is int):
            if (object.tok2index.__class__ is da.core.DataFrame) and (object.index2tok.__class__ is da.core.DataFrame):
                return 1
            if (object.tok2index.__class__ is dict) and (object.index2tok.__class__ is dict):
                return 2
        return 0
from dask import dataframe as da

def check_tok2index(object):
    return all([type(x) is int for x in object.tok2index.values()])

def check_index2tok(object):
    if any([type(x) is not int for x in object.index2tok.keys()]): 
        return False
    if all([type(x) is not str for x in object.index2tok.values()]): 
        return False
    return True

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
            try:
                print(check_index2tok(object))
                print(check_tok2index(object))
                if not (check_index2tok(object) and check_tok2index(object)): return 2
            except:
                return 0
            else:
                return 1
        return 0
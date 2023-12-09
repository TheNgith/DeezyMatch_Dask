from dask import dataframe as dd
from tqdm import tqdm
from numpy import ndarray

# ------------------- lookupToken --------------------
class lookupToken:
    """
    Create a lookup table for tokens
    """

    def __init__(self, name):
        self.name = name
        self.tok2index = {"_PAD": 0, "_UNK": 1}
        self.tok2count = {}
        self.index2tok = {0: "_PAD", 1: "_UNK"}
        self.n_tok = 2  # Count _PAD and _UNK

    def addTokens(self, list_tokens):
        list_tokens = list_tokens.compute()
        for i in tqdm(range(list_tokens.shape[0])):
            # tok = list_tokens[i].compute().tolist()
            tok = list_tokens[i]
            if tok not in self.tok2index.keys():
                self.tok2index[tok] = self.n_tok 
                ## if tok is integer by any chance, self.tok2index[tok] will fail
                self.tok2index[tok] = self.n_tok
                self.tok2count[tok] = 1
                self.index2tok[self.n_tok] = tok
                self.n_tok += 1
            else:
                self.tok2count[tok] += 1
#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import numpy as np
import os
import pandas as pd
import time
from tqdm import tqdm
from torch.utils.data import Dataset
from dask import dataframe as dd
from dask import array as da

from .utils import cprint, bc
from .utils import string_split
from .utils import normalizeString
from .dask_data_format_handler import handle_zarr, handle_lookupToken
from .lookupToken_class import lookupToken

# --- set seed for reproducibility
from .utils import set_seed_everywhere

set_seed_everywhere(1364)
# ------------------- csv_split_tokenize --------------------

def csv_split_tokenize(
    dataset_path,
    pretrained_vocab_path=None,
    n_train_examples=None,
    missing_char_threshold=0.5,
    train_prop=0.7,
    val_prop=0.15,
    test_prop=0.15,
    preproc_steps=(True, True, True, False),
    max_seq_len=100,
    mode="char",
    read_list_chars=False,
    csv_sep="\t",
):

    # --- read CSV file (dataset)
    cprint("[INFO]", bc.dgreen, "read CSV file: {}".format(dataset_path))

    # replaced by the following block
    # dataset_pd = pd.read_csv(dataset_path, sep=csv_sep, header=None, usecols=[0, 1, 2])
    # dataset_pd = dataset_pd.rename(columns={0: "s1", 1: "s2", 2: "label"})

    ds_fio = open(dataset_path, "r")
    df_list = da.from_array([])
    i=0
    for line in ds_fio:
        tmp_split_row = line.split(csv_sep)
        if str(tmp_split_row[2]).strip().lower() not in ["true", "false", "1", "0"]:
            print(f"SKIP: {line}")
            # change the label to remove_me,
            # we drop the rows with no true|false in the label column
            tmp_split_row = f"X{csv_sep}X{csv_sep}remove_me".split(csv_sep)
        line = list(map(lambda x: x.strip(), tmp_split_row[:3]))
        df_list = da.append(df_list, line)
        i+=1
    df_list = df_list.reshape(i, 3)
    dataset_pd = dd.from_dask_array(df_list, columns=["s1", "s2", "label"])

    # remove faulty rows
    dropping_index = dataset_pd[
            ~dataset_pd["label"].astype(str).str.contains("true|false", case=False)].index
    
    dataset_pd_index = range(0, i)
    
    dataset_pd = dataset_pd.loc[list(set(dataset_pd_index).difference(set(dropping_index)))]
    dataset_pd.label = dataset_pd.label.replace("(?i)TRUE", True, regex=True)
    dataset_pd.label = dataset_pd.label.replace("(?i)FALSE", False, regex=True)
    
    # count number of False and True
    counter = dataset_pd["label"].value_counts().compute().to_dict()
    if len(counter) < 2:
        if True not in counter.keys():
            counter[True] = 0
        if False not in counter.keys():
            counter[False] = 0
    num_true = counter[True]
    num_false = counter[False]
    print(counter)
    cprint(
        "[INFO]",
        bc.lgreen,
        "number of labels, True: {} and False: {}".format(num_true, num_false),
    )

    # --- splitting dataset
    t1 = time.time()
    cprint("[INFO]", bc.dgreen, "Splitting the Dataset")

    dataset_pd["split"] = "not_assigned"
    dataset_pd["original_index"] = dataset_pd.index

    dataset_split = dd.from_pandas(pd.DataFrame(), npartitions=1)
    for label in set(dataset_pd["label"]):
        rows_one_label = dataset_pd.loc[dataset_pd["label"] == label].copy()
        rows_one_label = rows_one_label.reset_index()
        n_total = len(rows_one_label)

        if n_train_examples:
            # We have two sets of labels: True and False
            # Here, we divide the number of requested rows by two
            # This way 50% of the requested rows will be True and 50% will be False
            # Compare this with n_train = int(train_prop * n_total)
            n_pos = int(int(n_train_examples) / 2)
            n_train = n_pos
        else:
            n_train = int(train_prop * n_total)

        n_val = int(val_prop * n_total)
        n_test = int(test_prop * n_total)

        rows_one_label.loc[:n_train, "split"] = "train"
        rows_one_label.loc[n_train : n_train + n_val, "split"] = "val"
        rows_one_label.loc[n_train + n_val : n_train + n_val + n_test, "split"] = "test"
        if n_train_examples is None:
            # if any remainder, assign to train
            rows_one_label.loc[
                rows_one_label["split"] == "not_assigned", "split"
            ] = "train"
        
        dataset_split = dd.concat([
                                   dataset_split, 
                                   rows_one_label
                                   ])
    cprint(
        "[INFO]",
        bc.dgreen,
        "finish splitting the Dataset. User time: {}".format((time.time() - t1)),
    )
    cprint(
        "[INFO]",
        bc.lgreen,
        "splits are as follow:\n{}".format(dataset_split["split"].value_counts()),
    )

    # clear the memory, continue with dataset_split
    del dataset_pd

    # --- create a lookup table, convert characters to indices
    cprint(
        "[INFO]",
        bc.dgreen,
        "start creating a lookup table and convert characters to indices",
    )
    dataset_split["s1_unicode"] = dataset_split["s1"].apply(
        normalizeString, args=preproc_steps
    )
    dataset_split["s2_unicode"] = dataset_split["s2"].apply(
        normalizeString, args=preproc_steps
    )

    cprint("[INFO]", bc.dgreen, "-- create vocabulary")
    dataset_split["s1_tokenized"] = dataset_split["s1_unicode"].apply(
        lambda x: da.from_array(string_split(
            x,
            tokenize=mode["tokenize"],
            min_gram=mode["min_gram"],
            max_gram=mode["max_gram"],
            token_sep=mode["token_sep"],
            prefix_suffix=mode["prefix_suffix"],
        )))
    dataset_split["s2_tokenized"] = dataset_split["s2_unicode"].apply(
        lambda x: da.from_array(string_split(
            x,
            tokenize=mode["tokenize"],
            min_gram=mode["min_gram"],
            max_gram=mode["max_gram"],
            token_sep=mode["token_sep"],
            prefix_suffix=mode["prefix_suffix"],
        )))
    
    s1_s2_flatten = dataset_split[["s1_tokenized", "s2_tokenized"]].to_dask_array(lengths=True).ravel()
    s1_s2_flatten_all_tokens = da.unique(da.hstack([s1_s2_flatten]))

    cprint("[INFO]", bc.dgreen, "-- convert tokens to indices")

    s1_tokenized = dataset_split["s1_tokenized"].to_dask_array(lengths=True)
    s2_tokenized = dataset_split["s2_tokenized"].to_dask_array(lengths=True)

    if pretrained_vocab_path:
        dataset_vocab = handle_lookupToken(pretrained_vocab_path)

        # XXX we need to document the following lines
        s1_indx = [
                    [
                        dataset_vocab.tok2index[tok.compute()]
                        for tok in seq.compute()
                        if tok.compute() in dataset_vocab.tok2index
                    ] 
                    for seq in tqdm(s1_tokenized)
                ]
        s2_indx = [
                    [
                        dataset_vocab.tok2index[tok.compute()]
                        for tok in seq.compute()
                        if tok.compute() in dataset_vocab.tok2index
                    ] 
                    for seq in tqdm(s2_tokenized)
                ]
        
        # Compute len(s1_indx) / len(s1_tokenized)
        # If this ratio is 1: all characters (after tokenization) could be found in the pretrained vocabulary
        # Else: some characters are missing. If "1 - (that ratio) > missing_char_threshold", remove the entry
        to_be_removed = []
        for i in range(len(s1_indx) - 1, -1, -1):
            s1_tokenized_i = s1_tokenized[i].compute()
            s2_tokenized_i = s2_tokenized[i].compute()
            if (
                (1 - len(s1_indx[i]) / max(1, s1_tokenized_i.shape[0])) >  missing_char_threshold
                or (1 - len(s2_indx[i]) / max(1, s2_tokenized_i.shape[0])) > missing_char_threshold
                or s1_tokenized_i.shape[0] == 0 
                or s2_tokenized_i.shape[0] == 0
            ):
                print(i, s1_indx[i], s1_tokenized_i)
                to_be_removed.append(i)
                del s1_indx[i]
                del s2_indx[i]

        cprint("[INFO]", bc.dgreen, "skipping {} lines".format(len(to_be_removed)))
       
        dataset_split = dataset_split.reset_index()
    
        dataset_split = dataset_split.repartition(npartitions=1)
      
        dataset_split.divisions = (0, dataset_split.shape[0].compute() - 1)
      
        dataset_split = dataset_split.loc[list(set(dataset_split.index).difference(set(to_be_removed)))]
        
        dataset_split["s1_indx"] = dd.from_array(np.array([[]] + s1_indx, dtype=object)[1:])
        dataset_split["s2_indx"] = dd.from_array(np.array([[]] + s2_indx, dtype=object)[1:])

    else:
        cprint("[INFO]", bc.dgreen, "-- create a lookup table for tokens")
        dataset_vocab = lookupToken("lookup_token")
        if read_list_chars:
            cprint(
                "[INFO]",
                bc.dgreen,
                f"-- read list of characters from {read_list_chars}",
            )
            dataset_vocab.addTokens(handle_zarr(read_list_chars))
        # Add additional tokens in the dataset, if any
        dataset_vocab.addTokens(s1_s2_flatten_all_tokens)
        cprint("[INFO]", bc.dgreen, f"-- Length of vocabulary: {dataset_vocab.n_tok}")

        dataset_split["s1_indx"] = dd.from_array(np.array([
                    [dataset_vocab.tok2index[tok.compute()] for tok in seq.compute() if tok.compute() in dataset_vocab.tok2index] 
                    for seq in tqdm(s1_tokenized)]))
        dataset_split["s2_indx"] = dd.from_array(np.array([
                    [dataset_vocab.tok2index[tok.compute()] for tok in seq.compute() if tok.compute() in dataset_vocab.tok2index] 
                    for seq in tqdm(s2_tokenized)]))

    # cleanup the indices
    dataset_split = dataset_split.reset_index(drop=True)

    with pd.option_context("mode.chained_assignment", None):
        train_dc = DatasetClass(
            dataset_split.loc[dataset_split["split"] == "train"],
            dataset_vocab,
            maxlen=max_seq_len,
        )
        valid_dc = DatasetClass(
            dataset_split.loc[dataset_split["split"] == "val"],
            dataset_vocab,
            maxlen=max_seq_len,
        )
        test_dc = DatasetClass(
            dataset_split.loc[dataset_split["split"] == "test"],
            dataset_vocab,
            maxlen=max_seq_len,
        )

    return train_dc, valid_dc, test_dc, dataset_vocab


# ------------------- test_tokenize --------------------
# XXX in future we could divide the previous function in two (split and tokenize)
# so that we have a single text processing function
def test_tokenize(
    dataset_path,
    train_vocab,
    missing_char_threshold=0.5,
    preproc_steps=(True, True, True, False),
    max_seq_len=100,
    mode="char",
    cutoff=None,
    save_test_class="./test_dc.df",
    save_test_class_dask="./test_dc.ddf",
    dataframe_input=False,
    csv_sep="\t",
    one_column_inp=False,
    verbose=True,
):
    
    if dataframe_input:
        if verbose:
            cprint("[INFO]", bc.dgreen, "use a dataframe in test_tokenize.")
        dataset_pd = dataset_path
    else:
        if verbose:
            cprint("[INFO]", bc.dgreen, "read CSV file: {}".format(dataset_path))
        ds_fio = open(dataset_path, "r")
        df_list = da.from_array([])
        i = 0
        for line in ds_fio:
            tmp_split_row = line.split(csv_sep)
            # If one_column_inp is set to True, extend the row
            if one_column_inp == True:
                # Copy the string of the first column into the second column
                # See issue 109, this way, we do not need to extend the vocabulary
                tmp_split_row.insert(1, tmp_split_row[0])
                tmp_split_row.insert(2, "true")

            if str(tmp_split_row[2]).strip().lower() not in ["true", "false", "1", "0"]:
                if verbose:
                    print(f"SKIP: {line}")
                # change the label to remove_me,
                # we drop the rows with no true|false in the label column
                tmp_split_row = f"X{csv_sep}X{csv_sep}remove_me".split(csv_sep)
            line = list(map(lambda x: x.strip(), tmp_split_row[:3]))
            df_list = da.append(df_list, line)
            i+=1
        df_list = df_list.reshape(i, 3)
        dataset_pd = dd.from_dask_array(df_list, columns=["s1", "s2", "label"])
        # dataset_pd = pd.read_csv(dataset_path, sep="\t", header=None, usecols=[0, 1, 2])
        # dataset_pd = dataset_pd.rename(columns={0: "s1", 1: "s2", 2: "label"})
        
    # XXX remove faulty rows
    dropping_index = dataset_pd[
            ~dataset_pd["label"].astype(str).str.contains("true|false", case=False)].index
    
    dataset_pd_index = range(0, i)
    
    dataset_pd = dataset_pd.loc[list(set(dataset_pd_index).difference(set(dropping_index)))]
    
    dataset_pd.label = dataset_pd.label.replace("(?i)TRUE", True, regex=True)
    dataset_pd.label = dataset_pd.label.replace("(?i)FALSE", False, regex=True)
    
    # count number of False and True
    counter = dataset_pd["label"].value_counts().compute().to_dict()
    if len(counter) < 2:
        if True not in counter.keys():
            counter[True] = 0
        if False not in counter.keys():
            counter[False] = 0
    num_true = counter[True]
    num_false = counter[False]
    
    if verbose:
        cprint(
            "[INFO]",
            bc.lgreen,
            "number of labels, True: {} and False: {}".format(num_true, num_false),
        )
    
    # instead of processing the entire dataset we first consider double the amount of the cutoff
    if cutoff == None:
        cutoff = dataset_pd.shape[0].compute()
    dataset_pd = dataset_pd.loc[: cutoff * 2]
    dataset_pd["s1_unicode"] = dataset_pd["s1"].apply(
        normalizeString, args=preproc_steps
    )
    dataset_pd["s2_unicode"] = dataset_pd["s2"].apply(
        normalizeString, args=preproc_steps
    )

    dataset_pd["s1_tokenized"] = dataset_pd["s1_unicode"].apply(
        lambda x: da.from_array(string_split(
            x,
            tokenize=mode["tokenize"],
            min_gram=mode["min_gram"],
            max_gram=mode["max_gram"],
            token_sep=mode["token_sep"],
            prefix_suffix=mode["prefix_suffix"],
        )))
    dataset_pd["s2_tokenized"] = dataset_pd["s2_unicode"].apply(
        lambda x: da.from_array(string_split(
            x,
            tokenize=mode["tokenize"],
            min_gram=mode["min_gram"],
            max_gram=mode["max_gram"],
            token_sep=mode["token_sep"],
            prefix_suffix=mode["prefix_suffix"],
        )))
    
    s1_tokenized = dataset_pd["s1_tokenized"].to_dask_array(lengths=True)
    s2_tokenized = dataset_pd["s2_tokenized"].to_dask_array(lengths=True)

    # XXX we need to explain why we have an if in the following for loop
    s1_indx = [
                    [
                        train_vocab.tok2index[tok.compute()]
                        for tok in seq.compute()
                        if tok.compute() in train_vocab.tok2index
                    ] 
                    for seq in tqdm(s1_tokenized)
                ]
    
    s2_indx = [
                [
                    train_vocab.tok2index[tok.compute()]
                    for tok in seq.compute()
                    if tok.compute() in train_vocab.tok2index
                ] 
                for seq in tqdm(s2_tokenized)
            ]
 
    # Compute len(s1_indx) / len(s1_tokenized)
    # If this ratio is 1: all characters (after tokenization) could be found in the pretrained vocabulary
    # Else: some characters are missing. If "1 - (that ratio) > missing_char_threshold", remove the entry
    to_be_removed = []
    for i in range(len(s1_indx) - 1, -1, -1):
        s1_tokenized_i = s1_tokenized[i].compute()
        s2_tokenized_i = s2_tokenized[i].compute()
        if (
            (1 - len(s1_indx[i]) / max(1, s1_tokenized_i.shape[0])) >  missing_char_threshold
            or (1 - len(s2_indx[i]) / max(1, s2_tokenized_i.shape[0])) > missing_char_threshold
            or s1_tokenized_i.shape[0] == 0 
            or s2_tokenized_i.shape[0] == 0
        ):
            print(i, s1_indx[i], s1_tokenized_i)
            to_be_removed.append(i)
            del s1_indx[i]
            del s2_indx[i]
    
    if verbose:
        cprint("[INFO]", bc.dgreen, "skipping {} lines".format(len(to_be_removed)))
    
    dataset_pd = dataset_pd.reset_index()
    
    dataset_pd = dataset_pd.repartition(npartitions=1)
    
    dataset_pd.divisions = (0, dataset_pd.shape[0].compute() - 1)
    
    dataset_pd = dataset_pd.loc[list(set(dataset_pd.index).difference(set(to_be_removed)))]
    
    dataset_pd = dataset_pd.reset_index(drop=True)
    dataset_pd = dataset_pd.repartition(npartitions=1)
    dataset_pd.divisions = (0, dataset_pd.shape[0].compute() - 1)
    dataset_pd["s1_indx"] = dd.from_array(np.array([[]] + s1_indx, dtype=object)[1:])
    dataset_pd["s2_indx"] = dd.from_array(np.array([[]] + s2_indx, dtype=object)[1:])

    # and then we do the cutoff again after having excluded the ones to be removed
    dataset_pd = dataset_pd.loc[:cutoff]

    # cleanup the indices
    dataset_pd = dataset_pd.reset_index(drop=True)

    with pd.option_context("mode.chained_assignment", None):
        test_dc = DatasetClass(dataset_pd, train_vocab, maxlen=max_seq_len)
    
    if save_test_class:
        if verbose:
            cprint(
                "[INFO]", bc.dgreen, "save test-data-class: {}".format(save_test_class)
            )
        abs_path = os.path.abspath(save_test_class)
        if not os.path.isdir(os.path.dirname(abs_path)):
            os.makedirs(os.path.dirname(abs_path))
        # test_dc.dask_df.to_parquet(save_test_class_dask, schema=test_dc.schema)
        test_dc.df.to_pickle(open(save_test_class, 'wb'))
    return test_dc


# ------------------- Dataframe2Class --------------------
class DatasetClass(Dataset):
    def __init__(self, dataset_split, dataset_vocab, maxlen=100):
        self.maxlen = maxlen
        self.df = dataset_split.compute()
        self.vocab = dataset_vocab.tok2index.keys()
        # self.schema = {
        #     's1_tokenized': pyarrow.list_(pyarrow.string()),
        #     's2_tokenized': pyarrow.list_(pyarrow.string()),
        #     's1_index': pyarrow.list_(pyarrow.int64()),
        #     's2_index': pyarrow.list_(pyarrow.int64())
        # }

        tqdm.pandas(desc="length s1", leave=False)
        self.df["s1_len"] = self.df.s1_indx.progress_apply(
            lambda x: self.maxlen if len(x) > self.maxlen else len(x)
        )
        tqdm.pandas(desc="length s2", leave=False)
        self.df["s2_len"] = self.df.s2_indx.progress_apply(
            lambda x: self.maxlen if len(x) > self.maxlen else len(x)
        )

        tqdm.pandas(desc="s1 padding", leave=False)
        self.df["s1_indx_pad"] = self.df.s1_indx.progress_apply(self.pad_data)
        tqdm.pandas(desc="s2 padding", leave=False)
        self.df["s2_indx_pad"] = self.df.s2_indx.progress_apply(self.pad_data)

        # # create word to index dictionary and reverse
        # self.token2idx = {o: i for i, o in enumerate(self.vocab)}
        # self.idx2token = {i: o for i, o in enumerate(self.vocab)}

    def __len__(self):
        return self.df.shape[0]

    def __getitem__(self, idx):
        X1 = self.df.s1_indx_pad.iloc[idx]
        len1 = self.df.s1_len.iloc[idx]

        X2 = self.df.s2_indx_pad.iloc[idx]
        len2 = self.df.s2_len.iloc[idx]

        y = int(self.df.label.iloc[idx])

        id2pass = self.df.index[idx]
        return X1, len1, X2, len2, y, id2pass

    def pad_data(self, s):
        padded = np.zeros((self.maxlen,), dtype=np.int64)
        if len(s) > self.maxlen:
            padded[:] = s[: self.maxlen]
        else:
            padded[: len(s)] = s
        return padded

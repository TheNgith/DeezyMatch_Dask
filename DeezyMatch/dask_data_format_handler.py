from dask import array as da
from dask import dataframe as dd
from check_lookupToken import islookupToken
from os import path

import pickle
import zarr

from utils import cprint, bc
from conversion import *

class notLookupToken(Exception): pass
class oldLookupToken(Exception): pass
class invalidLookupToken(Exception): pass

def handle_zarr(directory: str):
    try: data = da.from_zarr(directory)
    except zarr.errors.FSPathExistNotDir:
        cprint(
                "[DASK INFO]",
                bc.red,
                f"-- Not a zarray: {directory}",
            )
        
        cprint(
            "[DASK INFO]",
            bc.green,
            f"-- ...Trying to convert to zarray: {directory}",
        )
        try: new_directory = pickleToZarr(directory)
        except pickleFailed as e:
            cprint(
                "[DASK ERROR]",
                bc.red,
                f"-- Conversion failed: Pickle failed to read: {directory}",
            )
            raise e
        except DaskArrayFailed as e:
            cprint(
                "[DASK ERROR]",
                bc.red,
                f"-- Conversion failed: Must be a pickled List of Strings: {directory}",
            )
            raise e
        except zarr.errors.ContainsArrayError as e:
            try:
                try_path = path.splitext(directory)[0] + "_updated" + path.splitext(directory)[1]
                cprint(
                    "[DASK INFO]",
                    bc.yellow,
                    f"-- Pre-converted zarray might have existed, trying to use pre-converted: {try_path}",
                )
                new_data = handle_zarr(try_path)
            except: raise e
            else: 
                cprint(
                    "[DASK INFO]",
                    bc.yellow,
                    f"-- More up-to-date format of pretrained vocab file detected at {try_path}",
                )
                cprint(
                    "[DASK INFO]",
                    bc.yellow,
                    f"-- You might want to update the input file to save resource for future run sessions",
                )
                cprint(
                    "[DASK INFO]",
                    bc.green,
                    f"-- Current session continues...",
                )
                return new_data
        else:
            cprint(
                "[DASK INFO]",
                bc.green,
                f"-- Successfully converted to zarray at {new_directory}",
            )
            cprint(
                "[DASK INFO]",
                bc.green,
                f"-- Continue execution with the new file...",
            )
            return da.from_zarr(new_directory)    
    except Exception as e:
        cprint(
            "[DASK ERROR]",
            bc.red,
            f"-- Incorrect file format, ressolving attempt failed: {directory}",
        )
        raise e
    
    else:
        cprint(
                "[DASK INFO]",
                bc.green,
                f"-- Valid vocab file. Continue execution...",
            )
        return data

def handle_lookupToken(directory: str):
    try: data = pickle.load(open(directory, 'rb'))
    except Exception as e: raise e
    else:
        try:
            flag = islookupToken(data) 
            if flag:
                cprint(
                    "[DASK INFO]",
                    bc.green,
                    f"-- Valid lookupToken object: {directory}.",
                )
                if flag == 1:
                    return data
                if flag == 2:
                    raise oldLookupToken
            else: raise invalidLookupToken
        except invalidLookupToken as e:
            cprint(
                "[DASK ERROR]",
                bc.red,
                f"-- pretrained vocab must be stored as lookupToken object or alike",
            )
            raise e
        except oldLookupToken:
            cprint(
                "[DASK INFO]",
                bc.yellow,
                f"-- Old lookupToken object detected at {directory}",
            )
            try:
                cprint(
                    "[DASK INFO]",
                    bc.yellow,
                    f"-- Trying looking for the updated version in the same folder...",
                )
                try_path = path.splitext(directory)[0] + '_updated' + path.splitext(directory)[1]
                new_data = handle_lookupToken(try_path)
            except:
                try:
                    cprint(
                        "[DASK INFO]",
                        bc.yellow,
                        f"-- Couldn't find the updated version in the same folder",
                    )
                    cprint(
                        "[DASK INFO]",
                        bc.green,
                        f"-- Begin upgrading the old one...",
                    )
                    new_data, new_path = update_lookupToken(data, directory)
                except updatelookupTokenFailed as e:
                    cprint(
                        "[DASK INFO]",
                        bc.red,
                        f"Upgrade failed.",
                    )
                    raise e
                else:
                    cprint(
                        "[DASK INFO]",
                        bc.lgreen,
                        f"-- Upgrade succeeded.",
                    )
                    cprint(
                        "[DASK INFO]",
                        bc.green,
                        f"-- New vocab file stored at: {new_path}",
                    )
                    cprint(
                        "[DASK INFO]",
                        bc.yellow,
                        f"-- You might want to update pretrained_vocab_path in the input file to: {new_path}",
                    )
                    cprint(
                        "[DASK INFO]",
                        bc.green,
                        f"-- Continue execution...",
                    ) 
                    return new_data
            else: 
                cprint(
                    "[DASK INFO]",
                    bc.yellow,
                    f"-- More updated version of pretrained vocab found at {try_path}",
                )
                cprint(
                    "[DASK INFO]",
                    bc.yellow,
                    f"-- You might want to update the input file to save resources in next running sessions",
                )
                cprint(
                    "[DASK INFO]",
                    bc.green,
                    f"-- This session continues normally...",
                )
                return new_data
                
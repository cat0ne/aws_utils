import os
import io
from io import StringIO
from io import BytesIO
import pickle as pk
import pandas as pd
import boto3, botocore
import s3fs, fastparquet


# helper functions for CSV files read/write on S3_______________________________

def read_csv_s3(s3_bucket, path, sep=",", compression = None, **kwargs):
    """
    Read a csv file stored on AWS s3.
    :param s3_bucket: s3 bucket name
    :param path: key value to file
    :param sep: separator default value = ","
    :param compression:  None or gzip  - default (None)
    :param **kwargs : extra arguments for pd.read_csv
    """
    assert not "/" in s3_bucket
    assert not path.startswith("/")
    if compression == 'gzip':
        assert path.endswith(".csv.gz")
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=s3_bucket, Key=path)
        return pd.read_csv(BytesIO(obj['Body'].read()), sep=sep, compression='gzip', **kwargs)
    elif compression == None :
        assert path.endswith(".csv")
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=s3_bucket, Key=path)
        return pd.read_csv(BytesIO(obj['Body'].read()), sep=sep, **kwargs)
    else:
        print('Error in read_csv_s3: compression not recognized/implemented.\nCSV ompression: ',compression)

def write_csv_s3(s3_bucket, path, data, **kwargs):
    """
    Write a csv file on AWS s3.
    :param s3_bucket: s3 bucket name
    :param path: key value to file
    :param data pandas DataFrame
    :param **kwargs : extra arguments for pd.to_csv
    """
    with StringIO() as csv_buffer:
        data.to_csv(csv_buffer, **kwargs)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(s3_bucket, path).put(Body=csv_buffer.getvalue())

# helper functions for parquet files read/write on S3___________________________

def read_parquet_s3(s3_bucket, path, columns=None, skip_missing_colums=False):
    """
    Read a parquet file stored on AWS s3
    :param s3_bucket: s3 bucket name
    :param path: key value to file
    :param columns: load only those columns from the parquet file
    :param skip_missing_colums: skip columns specified if they are not in the parquet file
    """
    assert not "/" in s3_bucket
    assert not path.startswith("/")
    assert path.endswith(".parquet")
    s3 = s3fs.S3FileSystem()
    path = os.path.join(s3_bucket, path)
    pf = fastparquet.ParquetFile(path, open_with=s3.open)
    if columns is not None:
        missing_columns = []
        for col in columns:
            if col not in pf.dtypes:
                missing_columns.append(col)
        if missing_columns and not skip_missing_colums:
            raise ValueError("Missing columns in {path}: {missing_columns}")
        columns = [c for c in columns if c not in missing_columns]
    else:
        columns = [c for c in pf.dtypes if c!="autoFilledFields"]
    return pf.to_pandas(columns)

def write_parquet_s3(s3_bucket, path, data, compression='snappy'):
    """
    Write a single file to a parquet AWS s3
    :param s3_bucket: s3 bucket name
    :param path: key value to file
    :param data pandas DataFrame
    :param compression:  None or snappy  - default (snappy)
    """
    assert not "/" in s3_bucket
    assert path.endswith(".parquet")
    assert not path.startswith("/")
    myopen = s3fs.S3FileSystem().open
    fastparquet.write(os.path.join(s3_bucket, path),
          data, compression=compression, open_with=myopen, write_index=True)

# helper function to read xls files on S3_______________________________________
def read_excel_s3(s3_bucket, path):
    """
    Read a microsoft excel single from AWS s3
    :param s3_bucket: s3 bucket name
    :param path: key value to file
    """
    assert not "/" in s3_bucket
    assert not path.startswith("/")
    assert path.endswith(".xlsx")
    href = "s3n://" + s3_bucket + "/" + path
    return pd.read_excel(href)

# helper functions to dump/read pickle objects on S3____________________________

class PickleS3:
    """
    Class to handle pickle objects (dump/load) on AWS s3.
    :param bucket: AWS s3 bucket (optionnal)
    """
    def __init__(self, bucket=None):
        assert bucket is not None
        self.bucket = bucket

    def dump(self, model, path):
        """
        Dump a pickle object (e.g. sklearn model) on AWS s3.
        :param model: object to picklize, e.g. an sklearn model
        :param path: key value to file
        """
        io_buffer = io.BytesIO()
        pk.dump(model, io_buffer)
        s3_resource = boto3.resource('s3')
        # path equivalent to key
        s3_resource.Object(self.bucket, path).put(Body=io_buffer.getvalue())
        io_buffer.close()

    def load(self, path):
        """
        Load a pickle object (e.g. sklearn model) stored on AWS s3.
        :param model: object to picklize, e.g. an sklearn model
        :param path: key value to file
        """
        s3_resource = boto3.resource('s3')
        # path equivalent to key
        s3_resource.Object(self.bucket, path)
        body_string = s3_resource.Object(self.bucket, path).get()["Body"].read()
        return pk.loads(body_string)

# dbc explode

dbcexplode unpacks the notebooks of a Databricks .dbc archive file. Databricks' .dbc archive files can be saved from the Databricks application by exporting a notebook file or folder. You can extract the dbc file directly or unzip the notebooks out of the dbc file into individual notebooks.

## Usage
Unpack a dbc archive file directly (potentially containing multiple notebooks):

    python dbcexplode.py ./dbcdir/exported.dbc



# SFTP Client for RMA data

This library assists with loading your trialling data from SFTP directly into a jupyter notebook. 

Once you have received your user ID and `.ppk` key from the MRNSupport, you will be able to use the provided notebooks together with this library to get your first steps with our data.  

The main function you'll be interacting with is

```
   def download(
        asset_class: AssetClass,
        frequency: Frequency,
        start: datetime,
        end: datetime,
        output: T.Union[Output, str] = DATAFRAME_STR,
        buckets: T.Tuple[Bucket, ...] = (),
        template: str = DEFAULT_TEMPLATE,
    )
```

which will take your choice of asset class, period, and frequency and either download the data to disk or to memory.



## Commands to populate the data

```
python ccew_datasette https://register-of-charities.charitycommission.gov.uk/documents/34602/417919/Main+Monthly+Extract+zip+file.zip/881761e4-c0c5-aa0e-376e-a3070124041a?t=1604602947324
python oscr_datasette https://www.oscr.org.uk/umbraco/Surface/FormsSurface/CharityRegDownload
python oscr_datasette --no-recreate https://www.oscr.org.uk/umbraco/Surface/FormsSurface/CharityFormerRegDownload
```
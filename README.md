# maplib
A Rust-based implementation of [stOTTR](https://dev.spec.ottr.xyz/stOTTR/) with extensions for mapping asset structures based on the [Epsilon Transformation Language](https://www.eclipse.org/epsilon/doc/etl/). Implemented with [Apache Arrow](https://arrow.apache.org/) in Rust using [Pola.rs](https://www.pola.rs/). with a Python wrapper

## Mapping
We can easily map DataFrames to RDF-graphs using the Python library. 
```python
from maplib import Mapping
#We use polars dataframes instead of pandas dataframes. The api is pretty similar.
import polars as pl

#Define a stOttr document with a template:
doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    """

#Define a data frame to instantiate:
df = pl.DataFrame({"MyValue": [1, 2]})
#Create a mapping object
mapping = Mapping([doc])
#Expand the template using the data in the dataframe
mapping.expand("http://example.net/ns#ExampleTemplate", df)
#Export triples
triples = mapping.to_triples()
print(triples)
```

Results in:
```python
[<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "1"^^<http://www.w3.org/2001/XMLSchema#long>, 
 <http://example.net/ns#myObject> <http://example.net/ns#hasValue> "2"^^<http://www.w3.org/2001/XMLSchema#long>]
```

An example mapping is provided in [this jupyter notebook](https://github.com/magbak/maplib/tree/main/doc/rds_mapping.ipynb).
The Python API is documented [here](https://github.com/magbak/maplib/tree/main/doc/python_mapper_api.md)

## Installing pre-built wheels
From the latest [release](https://github.com/magbak/maplib/releases), copy the appropriate .whl-file for your system, then run e.g.:
```shell
pip install https://github.com/magbak/maplib/releases/download/v0.1.45/stottr-0.1.45-cp310-cp310-manylinux_2_31_x86_64.whl
```

All code is licensed to [Prediktor AS](https://www.prediktor.com/) under the Apache 2.0 license unless otherwise noted, and has been financed by [The Research Council of Norway](https://www.forskningsradet.no/en/) (grant no. 316656) and [Prediktor AS](https://www.prediktor.com/) as part of a PhD Degree.  

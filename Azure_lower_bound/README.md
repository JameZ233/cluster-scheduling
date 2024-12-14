Parallel:
The three major functions, CPlen, Twork, and ModCP, to calculate the measurement on the Azure Dataset.
CPlen uses topological sort as its helper. ModCP uses creating stage as its helper.

The whole work flow relies on applying the three major functions on each DAG, whose data is extracted 
at first before sending to the processing function. Furthermore, to have effective computation, the h3
suite VM on google cloud is leveraged. With 88 parallel threads, even large amount of data -- provided with
no bugs-- can be successfully.

Images:
The images are the artifacts of data analysis, which we use in the paper to sum up the data analysis result.

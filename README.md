# apache-beam-word-count-dataflow
Code to run pipeline on google cloud dataflow

If you want to check how it was done please see the youtube video at this [link](https://www.youtube.com/watch?v=MLaQESVdx54) 

# Run the pipeline in your laptop
`python3.7 pipeline.py --input input.txt --output count.txt`

# Copy the file from local to google cloud storage
`gsutil cp [your file.txt] gs://[your storage bucket]/input.txt`

# Run the pipeline on google cloud
`python3.7 pipeline.py --input gs://[your storage bucket]/input.txt --output gs://[your storage bucket]/count.txt --runner=DataflowRunner --project=[your project id] --job_name=wordcount --temp_location=gs://[your storage bucket]/temp --region=us-central1`

# Run the pipelinedb on google cloud
`python3.7 pipelinedb.py --input gs://[your storage bucket]/input.txt --runner=DataflowRunner --project=[your project id] --job_name=wordcount --temp_location=gs://[your storage bucket]/temp --region=us-central1`





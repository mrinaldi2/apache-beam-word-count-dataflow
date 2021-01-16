import logging
import argparse
import re
from past.builtins import unicode
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class CountWords(beam.PTransform):
  """A transform to count the occurrences of each word.
  A PTransform that converts a PCollection containing lines of text into a
  PCollection of (word, count) tuples.
  """
  def expand(self, pcoll):
    def count_ones(word_ones):
      (word, ones) = word_ones
      return (word, sum(ones))

    return (
        pcoll
        | 'split' >> (
            beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)).
            with_output_types(unicode))
        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(count_ones))

def run(argv=None, save_main_session=True):
    """Runs the debugging wordcount pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input file to process.')
    parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'Read' >> ReadFromText(known_args.input)
        count = lines | CountWords()
        count | "Write" >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
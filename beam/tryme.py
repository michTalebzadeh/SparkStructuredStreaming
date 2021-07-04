import apache_beam as beam
import ast

# The DoFn to perform on each element in the input PCollection.
class Split(beam.DoFn):
    def process(self, element):
        val = ast.literal_eval(element[1])
        output ='('+','.join(map(str, val.values())) + ')'
        return [output]

def run():
    p = beam.Pipeline('DirectRunner')
    (p | 'ReadMessage' >>  beam.io.textio.ReadFromTextWithFilename('./inputs.json')
                        | 'Processing' >> beam.ParDo(Split())
                        | 'Write' >> beam.io.WriteToText('./results.txt'))
    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()


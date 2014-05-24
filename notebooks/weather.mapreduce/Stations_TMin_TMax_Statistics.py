import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRWeather(MRJob):
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            if elements[0]=='station':
                yield (('header',1), 1)
            else:
                if elements[1] == 'TMIN':
                    num_defined = sum([e!='' for e in elements[3:]])
                    yield (('TMIN', num_defined), 1)
                elif elements[1] == 'TMAX':
                    num_defined = sum([e!='' for e in elements[3:]])
                    yield (('TMAX', num_defined), 1)
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            yield (('error', 1),  1)

    def combiner(self, key, values):
        yield (key, sum(values))
    
    def reducer(self, key, values):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (key, sum(values))
        
if __name__ == '__main__':
    MRWeather.run()
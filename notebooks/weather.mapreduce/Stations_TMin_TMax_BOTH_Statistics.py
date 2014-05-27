import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

threshold = 350

class MRWeather(MRJob):
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            num_defined = sum([e!='' for e in elements[3:]])
            if num_defined >= threshold:
                if elements[1] == 'TMIN':
                    yield ((elements[0],elements[2]), 1)
                elif elements[1] == 'TMAX':
                    yield ((elements[0],elements[2]), 1)
                    
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)

    def reducer(self, key, values):
        if sum(values) == 2: # have both TMAX and TMIN
            yield ("valid", 1)
    
    def final_reducer(self, key, values):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (key, sum(values))
    
    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer),
                 self.mr(reducer=self.final_reducer)]
    
if __name__ == '__main__':
    MRWeather.run()
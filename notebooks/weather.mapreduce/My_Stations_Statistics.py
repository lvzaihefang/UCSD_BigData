import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRWeather(MRJob):
    #@ECatch
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            if elements[0]=='station':
                out = ('header',1)
            else:
                number_defined=sum([e!='' for e in elements[3:]])
                out = (elements[0], number_defined)
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            out=('error',1)

        finally:
            yield out    
    #@ECatch
    def combiner(self, key, values):
        yield (key, sum(values))
    
   # @ECatch
    def reducer(self, key, values):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (key, sum(values))
        
if __name__ == '__main__':
    MRWeather.run()
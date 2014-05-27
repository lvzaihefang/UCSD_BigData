import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRWeather(MRJob):    
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            station = line.split('[')[1].split(',')[0]
            station = station.replace('"', '')
            data = line.split('[')[2].split(']')[0].split(',')
            num_defined = 0
            for e in data:
                if int(e) != 0:
                    num_defined = num_defined + 1
            yield (station, num_defined)
                    
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)

    def combiner(self, key, values):
            yield key, sum(values)
            
    def reducer(self, key, values):
            yield key, sum(values)
    
if __name__ == '__main__':
    MRWeather.run()
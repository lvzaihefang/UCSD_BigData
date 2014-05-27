import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
from numpy import *
import re
from sys import stderr

threshold = 3

class MRWeather(MRJob):
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            num_defined = sum([e!='' for e in elements[3:]])
            if num_defined >= threshold:
                if elements[1] == 'TMIN':
                    yield (elements[0],elements[2]), ('TMIN', elements[3:])
                elif elements[1] == 'TMAX':
                    yield (elements[0],elements[2]), ('TMAX', elements[3:])
                    
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)

    def reducer(self, key, values):
        self.increment_counter('MrJob Counters','reducer-all',1)
        lst = list(values)
        if len(lst) == 2:
            if lst[0][0]=='TMIN':
                data = lst[0][1]+lst[1][1]
            else:
                data = lst[1][1]+lst[0][1]
            good_data = []
            for e in data:
                if e != '':
                    good_data.append(int64(e))
                else:
                    good_data.append(0)
            yield (key, good_data)
    
if __name__ == '__main__':
    MRWeather.run()
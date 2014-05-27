import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr
import pandas as pd
import numpy as np

percentage_variance_explained = 0.99

class MRWeather(MRJob):
    def configure_options(self):
        super(MRWeather,self).configure_options()
        self.add_file_option('--station_groups')
        #self.add_passthrough_option('--groups',type='string', default='', help='...')
        
    def mapper_init(self):        
        self.dict = {}
        group = pd.DataFrame.from_csv(self.options.station_groups)
        for i in range(len(group)):
            self.dict[group.iloc[i,0]] = group.iloc[i,1]        
        
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            station = line.split('[')[1].split(',')[0]
            station = station.replace('"', '')
            id = self.dict[station]
            
            data = line.split('[')[2].split(']')[0].split(',')            
            for i in range(len(data)):
                data[i]=int(data[i])
                
            yield (id, data)
        
        except KeyError as e:
            stderr.write('Key was not found.\n')
            
        except Exception, e:
            self.increment_counter('MrJob Counters','mapper-error',1)
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            
    def reducer(self, key, values):
        try:
            self.increment_counter('MrJob Counters','reducer',1) 
            matrix = list(values)
            df = pd.DataFrame.from_records(matrix)
            
            ### Calculate the Convariance Matrix ###
            Mean=np.mean(df, axis=0).values
            (rows, columns)=np.shape(df)
            C=np.zeros([columns,columns])   # Sum
            for i in range(rows):
                row=df.iloc[i,:]-Mean
                outer=np.outer(row,row)
                C=C+outer
            cov=C/rows
            
            ### Use SVD to calculate Eigenvectors ###
            U,D,V=np.linalg.svd(cov)
            curve = np.cumsum(D[:])/sum(D)
            eigenvector_requried = len(D)
            for i in range(len(curve)):
                if curve[i]>=percentage_variance_explained:
                    eigenvector_requried = i+1
                    break            
                   
            yield key, (rows, eigenvector_requried)
            
        except Exception, e:
            self.increment_counter('MrJob Counters','reducer-error',1)
            print e
            
if __name__ == '__main__':
    MRWeather.run()
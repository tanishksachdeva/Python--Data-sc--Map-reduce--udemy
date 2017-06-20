from mrjob.job import MRJob

class MRMaxTemperature(MRJob):
    
    def MakeFahrenheit(self, tenthsOfCelsius): # using cels  to fahrenheit
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, _, line):#
        (location, date, type, data, x, y, z, w) = line.split(',')
        if (type == 'TMAX'):
            temperature = self.MakeFahrenheit(data)
            yield location, temperature # we only need location and temperature

    def reducer(self, location, temps):
        yield location, max(temps) #to get the max value out max is used , min can also be calculated


if __name__ == '__main__':
    MRMaxTemperature.run()
    
#!python maxtemp.py weather.csv
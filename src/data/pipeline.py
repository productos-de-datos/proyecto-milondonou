'''
Se llaman y ejecutan las funciones para la ingesta de datos, la transformación de los datos, la creación de las tablas únicas de precios
horarios, los cálculos de precios promedios diarios y los cálculos de precios promedios mensuales, a través del encadenamiento
de tareas de Luigi.
'''

"""
Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.


"""

import luigi
from luigi import Task, LocalTarget

class ingestdata(Task):

    def output(self):
        return LocalTarget('data_lake/landing/arc.txt')

    def run(self):
        from ingest_data import ingest_data
        with self.output().open("w") as outfile:
            ingest_data()

class transformdata(Task):

    def requires(self):
        return ingestdata()
    
    def output(self):
        return LocalTarget('data_lake/raw/archivo.txt')

    def run(self):
        from transform_data import transform_data
        with self.output().open("w") as outfile:
            transform_data()

class cleandata(Task):
    
    def requires(self):
        return transformdata()
    
    def output(self):
        return LocalTarget('data_lake/cleansed/archivo.txt')

    def run(self):
        from clean_data import clean_data
        with self.output().open("w") as outfile:
            clean_data()

class computedailyprices(Task):
    
    def requires(self):
        return cleandata()
    
    def output(self):
        return LocalTarget('data_lake/business/archivo.txt')

    def run(self):
        from compute_daily_prices import compute_daily_prices
        with self.output().open("w") as outfile:
            compute_daily_prices()

class computemonthlyprices(Task):
    
    def requires(self):
        return computedailyprices()
    
    def output(self):
        return LocalTarget('data_lake/business/archivo1.txt')

    def run(self):
        from compute_monthly_prices import compute_monthly_prices
        with self.output().open("w") as outfile:
            compute_monthly_prices()

if __name__ == "__main__":
    luigi.run(["computemonthlyprices", "--local-scheduler"])
    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    doctest.testmod()

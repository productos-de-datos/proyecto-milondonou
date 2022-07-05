"""
En este modulos se crea el lago de datos, con sus diferentes subcarpetas, en donde podrán almacenarse
todos los datos estructurados y no estructurados a cualquier escala. 

"""

def proyecto_principal():
    from pathlib import Path
    return Path(__file__).parent.parent.parent

def create_data_lake():
    """Cree el data lake con sus capas.

    Esta función debe crear la carpeta `data_lake` en la raiz del proyecto. El data lake contiene
    las siguientes subcarpetas:

    ```
    .
    |
    \___ data_lake/
         |___ landing/
         |___ raw/
         |___ cleansed/
         \___ business/
              |___ reports/
              |    |___ figures/
              |___ features/
              |___ forecasts/

    ```


    """
    import os 
    
    principal = proyecto_principal()
    os.mkdir(os.path.join(principal, "data_lake"))
    os.mkdir(os.path.join(principal, "data_lake/landing"))
    os.mkdir(os.path.join(principal, "data_lake/raw"))
    os.mkdir(os.path.join(principal, "data_lake/cleansed"))
    os.mkdir(os.path.join(principal, "data_lake/business"))
    os.mkdir(os.path.join(principal, "data_lake/business/reports"))
    os.mkdir(os.path.join(principal, "data_lake/business/features"))
    os.mkdir(os.path.join(principal, "data_lake/business/forecasts"))
    os.mkdir(os.path.join(principal, "data_lake/business/reports/figures"))

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest
    create_data_lake()
    doctest.testmod()


import calkulate as calk
import numpy as np
import pandas as pd

vindta = []
for vpath in ["labdata/Furious George", "labdata/R2-CO2"]:
    vfile = vpath + "/MaMa2023.dbs"

    # Import _vindta .dbs file
    _vindta = calk.read_dbs(vfile)
    # _vindta.to_excel(vpath + ' metadata.xlsx')

    # Add CRM details
    crm198 = {
        "dic": 2033.64,
        "alkalinity_certified": 2200.67,
        "salinity": 33.504,
        "total_phosphate": 0.67,
        "total_silicate": 3.8,
    }
    for k, v in crm198.items():
        _vindta[k] = np.where(_vindta.bottle.str.startswith("CRM"), v, np.nan)

    # Add junk guess values
    junk = {
        "dic": 2300,
        "salinity": 33,
        "total_phosphate": 1,
        "total_silicate": 5,
    }
    for k, v in junk.items():
        _vindta[k] = np.where(_vindta.bottle.str.startswith("JUNK"), v, _vindta[k])

    # Import sample metadata
    vmeta = pd.read_excel(vpath + " metadata.xlsx")

    # Convert per L to per kg
    vmeta["density"] = calk.density.seawater_1atm_MP81(
        temperature=23, salinity=vmeta.salinity
    )
    vcols = [c for c in vmeta.columns if not c == "bottle"]
    ncols = [c for c in vcols if c.endswith("_vol")]
    for c in ncols:
        vmeta[c.replace("_vol", "")] = vmeta[c] / vmeta.density
    for c in vcols:
        if c not in vmeta:
            vmeta[c] = np.nan

    for i, row in vmeta.iterrows():
        L = _vindta.bottle == row.bottle
        assert L.sum() == 1
        for c in vcols:
            _vindta.loc[L, c.replace("_vol", "")] = row[c.replace("_vol", "")]

    # Set file path
    _vindta["file_path"] = vpath + "/MaMa2023/"

    # Calibrate and solve alkalinity
    _vindta.calkulate()

    # Merge files
    vindta.append(_vindta.to_pandas())
vindta = calk.Dataset(pd.concat(vindta).reset_index())

# Save to file
vindta.to_excel('results/vindta.xlsx')

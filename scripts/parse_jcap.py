import h5py    
import numpy as np    
import pandas as pd
import os
import cv2
import json
import csv


data = h5py.File('dataset_comp_image_spectra.h5','r+')
# List all groups
a_group_key = list(data.keys())[0]

# Get the data
data_keys = list(data[a_group_key])

atfrac = pd.DataFrame(data['atfrac'].value)
at_frac_keys = data['atfrac_keys'].value
elts_frac_keys = [i[:i.index(b'.PM')].decode('utf-8') for i in at_frac_keys]

spectra = data['spectra'].value
energy_eV = data['energy_eV'].value
images = data['images'].value

for i in range(len(spectra)):
    full_dict = {}
    fractions = atfrac[i].tolist()
    #concentrations = {}
    #for e in range(len(elts_frac_keys)):
    #    concentrations[elts_frac_keys[e]]=fractions[e]
    elements = []
    elemental_proportions = []
    composition = ''
    for e in range(len(elts_frac_keys)):
        if float(fractions[e])!=0.0:
            composition+=str(elts_frac_keys[e])+str(fractions[e])
            elements.append(elts_frac_keys[e])
            elemental_proportions.append(fractions[e])
    energy = energy_eV
    spectrum = spectra[i]
    image = np.array(images[i])
    image = np.array(image*255., dtype=np.int64)
    full_dict['sample number']=i
    full_dict['composition']=composition
    full_dict['elements']=elements
    full_dict['elemental_proportions']=elemental_proportions
    path = 'test_'+str(i)+'/'
    os.mkdir(path)
    json.dump(full_dict,open(path+'test_'+str(i)+'.json','w'))
    cv2.imwrite(path+'test_'+str(i)+'.png',image)
    df = pd.DataFrame(np.array([energy,spectrum]).T,\
                      columns=['Energy','Intensity'])
    df.to_csv(path+'test_'+str(i)+'.csv',index=False)
    raise ValueError
    

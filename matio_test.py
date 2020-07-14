from materials_io.utils.interface import run_all_parsers_on_group
import mdf_matio

x = run_all_parsers_on_group(group=['/Users/tylerskluzacek/Desktop/matio-data/INCAR',
                                    '/Users/tylerskluzacek/Desktop/matio-data/OUTCAR',
                                    '/Users/tylerskluzacek/Desktop/matio-data/POSCAR'], include_parsers=['dft'], adapter_map='match')



print(list(x))

for item in x:
    print(item.metadata)
print("Done printing all items")
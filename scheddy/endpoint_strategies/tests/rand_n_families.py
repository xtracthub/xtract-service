
from scheddy.endpoint_strategies.rand_n_families import RandNFamiliesStrategy

rnf_strat = RandNFamiliesStrategy(n=0.4)

families = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6']
no_families = []
fx_eps_1 = {'a': {'workers': 1}}
fx_eps_2 = {'a': {'workers': 1}, 'b': {'workers': 1}}
fx_eps_3 = {'a': {'workers': 1}, 'b': {'workers': 1}, 'c': {'workers': 1}}
fx_eps_4 = dict()

print(f"\nOffload 40%...")
schedule = rnf_strat.schedule(fx_eps=fx_eps_1, families=families)
print(f"Schedule when 1 worker on 1 ep: {schedule}")
schedule = rnf_strat.schedule(fx_eps=fx_eps_2, families=families)
print(f"Schedule when 1 worker on 2 ep: {schedule}")
schedule = rnf_strat.schedule(fx_eps=fx_eps_3, families=families)
print(f"Schedule when 1 worker on 3 ep: {schedule}")

print("\n\n")

print(f"Offload everything...")
rnf_strat = RandNFamiliesStrategy(n=1)
schedule = rnf_strat.schedule(fx_eps=fx_eps_1, families=families)
print(f"Schedule when 1 worker on 1 ep: {schedule}")
schedule = rnf_strat.schedule(fx_eps=fx_eps_2, families=families)
print(f"Schedule when 1 worker on 2 ep: {schedule}")
schedule = rnf_strat.schedule(fx_eps=fx_eps_3, families=families)
print(f"Schedule when 1 worker on 3 ep: {schedule}")

print("\n\n")

print(f"Offload nothing...")
rnf_strat = RandNFamiliesStrategy(n=0)
schedule = rnf_strat.schedule(fx_eps=fx_eps_1, families=families)
print(f"Schedule when 1 worker on 1 ep: {schedule}")
schedule = rnf_strat.schedule(fx_eps=fx_eps_2, families=families)
print(f"Schedule when 1 worker on 2 ep: {schedule}")
schedule = rnf_strat.schedule(fx_eps=fx_eps_3, families=families)
print(f"Schedule when 1 worker on 3 ep: {schedule}")

print("\n\n")

print(f"Edge: no families...")
rnf_strat = RandNFamiliesStrategy(n=1)
schedule = rnf_strat.schedule(fx_eps=fx_eps_3, families=no_families)
print(f"Schedule when no families: {schedule}")

print("\n\n")

print(f"Edge: no workers...")
rnf_strat = RandNFamiliesStrategy(n=1, debug=False)
schedule = rnf_strat.schedule(fx_eps=fx_eps_4, families=families)
print(f"Schedule when no workers: {schedule}")

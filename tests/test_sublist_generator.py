
from orchestrator.orch_utils.utils import create_list_chunks

ls = [1, 2, 3, 4, 5, 6]

x = create_list_chunks(ls, 3)

for item in x:
    print(item)
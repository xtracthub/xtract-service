

# Create evenly sized sub-list chunks.
# https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def create_list_chunks(l, n):
    n = max(1, n)
    return (l[i:i + n] for i in range(0, len(l), n))
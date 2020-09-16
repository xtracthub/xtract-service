
import pickle as pkl
import json
import base64

def min_hash(fpath):
    """
    Extracts MinHash digest of a file's bytes

    fpath (str): path to file to extract MinHash of
    """

    from datasketch import MinHash

    NUM_PERMS = 128
    CHUNK_SZ = 64

    mh = MinHash(num_perm=NUM_PERMS)

    with open(fpath, 'rb') as of:
        print("File is open")
        count = 0
        by = of.read(CHUNK_SZ)
        while by != b"":
            by = of.read(CHUNK_SZ)
            count += 1
            mh.update(by)

    return mh


x = min_hash('/Users/tylerskluzacek/Desktop/github_avatar.jpg')
x_pkl = pkl.dumps(x)

# to_json = {'min_hash': x_pkl}
# print(x_pkl)

y = base64.b64encode(x_pkl)
y2 = y.decode('ascii')

to_json = {'min_hash': y2}

z = json.dumps(to_json)

### Now show that we can open it in minhash form.
a = json.loads(z)
b = base64.b64decode(a['min_hash'])
c = pkl.loads(b)
print(c)
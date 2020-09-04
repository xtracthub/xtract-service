
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
print(x)
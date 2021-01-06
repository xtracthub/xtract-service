
# Educational note:
# Iterators aren't REALLY meant to be appended to once they're 'generated'.
# Iterators are quite difficult to JSON serialize (you pretty much turn them back into lists...)


class BaseGenerator:
    def __init__(self, chunks):
        self.chunks = chunks

    def __iter__(self):
        for chunk in self.chunks:
            yield chunk


class GeneratorWrapper:
    def __init__(self):
        # self.family = family
        self.gen = None

    def pack_generator(self, chunks):
        assert(type(chunks) == list, "Type Chunks is list!")
        self.gen = BaseGenerator(chunks)

    def to_dict(self):

        if self.gen is not None:
            x = list(iter(self.gen))
            return x
        else:
            return None

    def from_dict(self):
        pass  # TODO.
        # TODO: also add byte decompression.


# a = GeneratorWrapper()
# a.pack_generator([1,2,3,4,5])
# b = a.to_dict()
# # print(b)
# # exit()
#
#
# # # TODO: keep this here as this is how we'll use.
# # f = list(iter(XtractGenerator([1, 2, 3, 4, 5])))
# import json
# # f = json.dumps((i*i for i in range(10)), iterable_as_array=True)
#
#
# from funcx import FuncXClient
#
# fxc = FuncXClient()
#
# funcx_uuid = fxc.register_function(function=double)
# print(funcx_uuid)
#
# funcx_try = fxc.run(b, function_id=funcx_uuid, endpoint_id='9c076997-e501-4f41-8508-c6713225d19d')
# print(funcx_try)
#
# while True:
#
#     a = fxc.get_batch_status([funcx_try])
#     print(a)
#
#     if 'exception' in a[funcx_try]:
#         a[funcx_try]['exception'].reraise()
#     time.sleep(1)

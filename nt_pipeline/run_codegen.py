from grpc_tools import protoc

import os


fnames = os.listdir('.')

files = []
for fname in fnames:
    if fname.split('.')[-1] != 'proto':
        continue
    files.append(fname)


params = ["-I.", "--python_out=./"]
params.extend(files)
protoc.main(params)

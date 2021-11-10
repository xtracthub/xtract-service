
# TODO: Remove this old way of doing the mapping
# TODO: Ideally move this to a shared DB that's called on startup (registration and running should be separate repos)
# mapping = {
#     'xtract-matio::midway2':
#         {'func_uuid': '7faf21be-4667-447c-a96d-4dbe14875cf1',
#          'location': 'xtract-matio.img',
#          'container_type': 'singularity',
#          'ep_id': '7faf21be-4667-447c-a96d-4dbe14875cf1',
#          'data_path': '/project2/chard/skluzacek/inposoutcars/'},
#     'xtract-matio::theta':
#         {'func_uuid': 'abec00cf-efd3-492e-a0c3-5c659dd06cdd',  # TODO: update this.
#          'location': '/projects/CSC249ADCD01/skluzacek/containers/xtract-matio.img',
#          'container_type': 'singularity',
#          'ep_id': 'adf8837a-8944-49fa-a877-7755915c61d6',
#          'data_path': '/projects/CSC249ADCD01/skluzacek/'},
#
#     'xtract-matio::js':
#
#         {'func_uuid': 'abec00cf-efd3-492e-a0c3-5c659dd06cdd',  # TODO: update this.
#          'location': '/home/tskluzac/xtract-matio.img',
#          'container_type': 'singularity',
#          'ep_id': '5da324b8-e8de-43d1-b4ca-d8f1d96c2d9b',
#          'data_path': '/projects/CSC249ADCD01/skluzacek/'},
#     'xtract-matio::riverk8s': "TODO",
# }

extractor_to_sing_container_name_map = {
    'tabular': 'xtract-tabular.img',
    'keyword': 'xtract-keyword.img',
    'netcdf': 'xtract-netcdf.img',
    'hdf': 'xtract-hdf.img',
    'images': 'xtract-images.img'
}
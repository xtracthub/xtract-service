from extractors.extractor import Extractor


class ImageExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         # TODO 1: Get this by running test_image_families.py. Don't do until you think it might work :)
                         func_id="f22d9206-0352-449f-8a5a-f6855f2bef30",
                         # TODO 2: Call yours xtract-c-code.
                         extr_name="xtract-image",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-image:latest")
        super().set_extr_func(images_extract)


def images_extract(event):
    # import platform
    # print(platform.python_version())

    import os
    import sys
    import time
    from shutil import copyfile

    from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader

    sys.path.insert(1, '/app')

    # return "Made it here 2"

    import xtract_images_main

    # return ("HEY")

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
            by = of.read(CHUNK_SZ)
            while by != b"":
                by = of.read(CHUNK_SZ)
                mh.update(by)

        return mh

    def imbytes_to_imformat(fb):

        """
        Takes bytes from an image and turns it into a representation
        we can use for classification
        """

        RESNET_SIZE = (224, 224)

        import tensorflow as tf
        from PIL import Image
        import io

        image = Image.open(io.BytesIO(fb))
        img_arr = tf.keras.preprocessing.image.img_to_array(image)
        if img_arr.shape[-1] == 1:
            img_arr = tf.tile(img_arr, [1, 1, 3])
        elif img_arr.shape[-1] == 4:
            img_arr = img_arr[:, :, :3]
        img_arr = tf.image.resize(img_arr[tf.newaxis, :, :, :], RESNET_SIZE)

        return img_arr

    def conv_resnet_labels(pred_obj):

        """
        Prediction object looks like:

    [
       [
          ('n07753592', 'banana', 0.99229723),
          ('n03532672', 'hook', 0.0014551596),
          ('n03970156', 'plunger', 0.0010738898),
          ('n07753113', 'fig', 0.0009359837) ,
          ('n03109150', 'corkscrew', 0.00028538404)
       ]
    ]

        And we want to get the labels from each.

        """

        import tensorflow as tf

        decoded_pred = tf.keras.applications.imagenet_utils.decode_predictions(pred_obj)
        unbatch = decoded_pred[0]
        get_pred_obj = lambda x: x[1]
        labels = [get_pred_obj(o) for o in unbatch]

        return labels

    def conv_to_web_labels(labels):

        """
        We're stuck with a rough legacy format--web labels are formatted
        in the database as a string

        '[(label, None)]'

        So we have to convert to that from the labels

        """

        return [(l, None) for l in labels]

    def get_im_model():

        import os
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '0'
        import tensorflow as tf

        # return "Made it here"
        resnet = tf.keras.applications.resnet50.ResNet50(weights='imagenet')
        # resnet = tf.keras.models.load_model('/app/resnet50_weights_tf_dim_ordering_tf_kernels.h5')


        out_layer = resnet.layers[-2]
        identity = tf.keras.layers.Lambda(lambda x: x)(out_layer.output)
        pred_layer = resnet.layers[-1](out_layer.output)

        model = tf.keras.models.Model(inputs=resnet.input,
                                      outputs=[identity, pred_layer])

        for l in model.layers:
            l.trainable = False

        return model

    def get_fb(fname):

        with open(fname, 'rb') as of:
            fb = of.read()
        return fb

    def finalize_im_rep(fname):

        fb = get_fb(fname)
        model = get_im_model()


        im = imbytes_to_imformat(fb)
        im_rep, label_preds = model.predict(im)
        full_labels = conv_resnet_labels(label_preds)
        return im_rep[0], full_labels

        # except Exception as e:
        #     print(e)
        #     return None, None


    import sys
    import pickle as pkl
    import base64

    # logging.error("Testing")
    # return "Made it here!"

    t0 = time.time()

    try:
        import cv2
    except Exception as e:
        return e

    cur_ls = os.listdir('.')
    if 'pca_model.sav' not in cur_ls or 'clf_model.sav' not in cur_ls:
        # TODO: Make these lines unnecessary.
        copyfile('/app/pca_model.sav', f'pca_model.sav')
        copyfile('/app/clf_model.sav', f'clf_model.sav')

    family_batch = event["family_batch"]
    creds = event["creds"]

    download_file = event["download_file"]

    if download_file:

        downloader = GoogleDriveDownloader(auth_creds=creds)

        # TODO: Put time info into the downloader/extractor objects.
        ta = time.time()
        try:
            downloader.batch_fetch(family_batch=family_batch)
        except Exception as e:
            return e
        tb = time.time()

        file_paths = downloader.success_files
        # return file_paths

        if len(file_paths) == 0:
            return {'family_batch': family_batch, 'error': True, 'tot_time': time.time()-t0,
                    'err_msg': "unable to download files"}

    # return "HEREZO"
    for family in family_batch.families:

        # return "NICE AND EASY. "
        img_path = family.files[0]['path']

        # return "IS THIS THE INDEX ISSUE"

        new_mdata = xtract_images_main.extract_image('predict', img_path)


        # TODO: BRING THESE BACK FOR MIDWAY.
        # new_mdata["min_hash"] = base64.b64encode(pkl.dumps(min_hash(img_path))).decode('ascii')
        # vec_rep, labels = finalize_im_rep(img_path)  # TODO: was fname

        # new_mdata['image_vector'] = vec_rep
        # new_mdata['image_objects'] = labels
        family.metadata = new_mdata

        # return new_mdata

    t1 = time.time()

    if download_file:
        [os.remove(file_path) for file_path in downloader.success_files]
    else:
        # This is the transfer time.
        ta = tb = 0

    return {'family_batch': family_batch, 'tot_time': t1-t0, 'trans_time': tb-ta}

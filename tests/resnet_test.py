import tensorflow as tf

resnet = tf.keras.applications.resnet50.ResNet50(weights='imagenet')
x = resnet.summary()


print(f"This is x: {x}")